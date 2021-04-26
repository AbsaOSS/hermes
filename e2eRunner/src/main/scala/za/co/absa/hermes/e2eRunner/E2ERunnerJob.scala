/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hermes.e2eRunner

import org.apache.spark.SPARK_VERSION

import java.io.File
import za.co.absa.hermes.e2eRunner.logging.LoggingFunctions
import za.co.absa.hermes.e2eRunner.logging.functions.Scribe
import za.co.absa.hermes.e2eRunner.plugins.FailedPluginResult
import za.co.absa.hermes.utils.SparkCompatibility

import scala.util.{Failure, Success, Try}

object E2ERunnerJob {
  /**
   * To always add plugins from this project we add this JAR file's path to the list of class paths
   */
  private val locationToThisClass = new File(
    this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI
  ).getPath

  def main(args: Array[String]): Unit = {
    SparkCompatibility.checkVersion(SPARK_VERSION)

    val cmd = E2ERunnerConfig.getCmdLineArguments(args) match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }

    implicit val loggingFunctions: Scribe = Scribe(this.getClass)

    val classPaths = (List(locationToThisClass) ++ cmd.jarPath).map(new File(_))
    loggingFunctions.info(
      s"""Plugin classes will be loaded from these paths:
         |${classPaths.mkString("\n")}""".stripMargin)

    val pluginDefinitions: PluginDefinitions = PluginDefinitions(classPaths)
    val pluginNames = pluginDefinitions.getPluginNames
    loggingFunctions.info(
      s"""Loaded plugins are:
         |${pluginNames.mkString("\n")}""".stripMargin)

    val testDefinitions = TestDefinitions.fromFile(cmd.testDefinition, cmd.extraVars)
    testDefinitions.ensureOrderAndDependenciesCorrect()
    loggingFunctions.info(s"Loaded ${testDefinitions.size} test definitions")

    val pluginsExpectedToUse = testDefinitions.getPluginNames
    validatePluginsToBeUsed(pluginNames, pluginsExpectedToUse)

    loggingFunctions.info("Running tests")
    val results = runTests(testDefinitions, pluginDefinitions, cmd.failFast)

    logFinalResultsUsingScribe(results)
  }

  /**
   * Runs test definitions one by one
   *
   * @param testDefinitions Tests to be run
   * @param pluginDefinitions Plugins available
   * @param failFast Should the tests fail if one of them throws exception
   * @return Returns a sequence of PluginResults
   */
  def runTests(testDefinitions: TestDefinitions,
               pluginDefinitions: PluginDefinitions,
               failFast: Boolean = false)(implicit loggingFunctions: LoggingFunctions): Seq[PluginResult] = {
    testDefinitions.foldLeftWithIndex {
      case (acc, TestDefinitionWithOrder(td, order)) =>
        loggingFunctions.info(s"Running ${td.name}")
        val plugin: Plugin = pluginDefinitions.getPlugin(td.pluginName)

        val tryExecution = if (canTestProceed(td, acc)) {
          tryExecute(td, order, plugin)
        } else {
          Failure(DependeeFailed(td.name, td.dependsOn.get))
        }

        tryExecution match {
          case Success(value) => acc :+ value
          case Failure(exception) if !failFast =>  acc :+ FailedPluginResult(td.args, exception, order, td.name)
          case Failure(exception) if failFast =>  throw TestFailedWithFailFastOn(td.name, order, exception)
        }
    }
  }

  private def logFinalResultsUsingScribe(results: Seq[PluginResult])(implicit loggingFunctions: LoggingFunctions): Unit = {
    loggingFunctions.info("##################################################")
    loggingFunctions.info("Invoking logging of test results")
    loggingFunctions.info("##################################################")
    results.foreach({ result =>
      result.resultLog.log
      loggingFunctions.info("##################################################")
    })
  }

  private def canTestProceed(td: TestDefinition, previousResults: Seq[PluginResult]): Boolean = {
    td.dependsOn.forall { dependerName =>
      val found = previousResults.find({ previousResult =>
        dependerName.equalsIgnoreCase(previousResult.getTestName)
      })
      if (found.isEmpty){ false }
      else { found.forall(_.testPassed) }
    }
  }

  private def tryExecute(td: TestDefinition,
                         testOrder: Int,
                         plugin: Plugin): Try[PluginResult] = Try {
    val result: PluginResult = plugin.performAction(td, testOrder)
    if (td.writeArgs.isDefined) result.write(td.writeArgs.get)
    result
  }

  /**
   * Validates if all the needed plugins are there
   * @param pluginNames Names of loaded plugins
   * @param pluginsExpectedToUse Names of plugins from all test definitions
   */
  private def validatePluginsToBeUsed(pluginNames: Set[String], pluginsExpectedToUse: Set[String]): Unit = {
    if (pluginsExpectedToUse.diff(pluginNames).nonEmpty)
      throw NotAllRequestedPluginsDefined(pluginNames.diff(pluginsExpectedToUse))
  }
}
