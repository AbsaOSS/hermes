/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hermes.e2eRunner

import java.io.File

import scala.util.{Failure, Success, Try}

object E2ERunnerJobExperimental {
  /**
   * To always add plugins from this project we add this JAR file's path to the list of class paths
   */
  private val locationToThisClass = new File(
    this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI
  ).getPath

  def main(args: Array[String]): Unit = {
    val cmd = E2ERunnerConfigExperimental.getCmdLineArguments(args) match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }

    val classpath = (List(locationToThisClass) ++ cmd.jarPath).map(new File(_))
    scribe.info(
      s"""Plugin classes will be loaded from these paths:
         |${classpath.mkString("\n")}""".stripMargin)

    val pluginDefinitions: PluginDefinitions = PluginDefinitions(classpath)
    val pluginNames = pluginDefinitions.getPluginNames
    scribe.info(
      s"""Loaded plugins are:
         |${pluginNames.mkString("\n")}""".stripMargin)

    val testDefinitions = TestDefinition.fromFile(cmd.testDefinition)
    scribe.info(s"Loaded ${testDefinitions.size} test definitions")

    val pluginsExpectedToUse = testDefinitions.getPluginNames
    validatePluginsToBeUsed(pluginNames, pluginsExpectedToUse)

    scribe.info("Running tests")
    val results = runTests(testDefinitions, pluginDefinitions, cmd.failfast)

    scribe.info("##################################################")
    scribe.info("Invoking logging of test results")
    scribe.info("##################################################")
    results.foreach({ x =>
      x.logResult()
      scribe.info("##################################################")
    })
  }

  /**
   * Runs test definitions one by one
   * @param testDefinitions Tests to be run
   * @param pluginDefinitions Plugins available
   * @param failFast Should the tests fail if one of them throws exception
   * @return Returns a sequence of PluginResults
   */
  def runTests(testDefinitions: TestDefinitions,
               pluginDefinitions: PluginDefinitions,
               failFast: Boolean = false): Seq[PluginResult] = {
    testDefinitions.foldLeftWithIndex {
      case (acc, TestDefinitionWithOrder(td, order)) =>
        scribe.info(s"Running ${td.name}")
        val plugin: Plugin = pluginDefinitions.getPlugin(td.pluginName)

        val tryExecution = if (canTestProceed(td, acc)) {
          tryExecute(td, order, plugin)
        } else {
          Failure(throw DependeeFailed(td.name, td.dependsOn.get))
        }

        tryExecution match {
          case Success(value) => acc :+ value
          case Failure(exception) if !failFast =>  acc :+ FailedPluginResult(td.args, exception, order, td.name)
          case Failure(exception) if failFast =>  throw TestFailedWithFailFastOn(td.name, order, exception)
        }
    }
  }

  private def canTestProceed(td: TestDefinition, previousResults: Seq[PluginResult]): Boolean = {
    td.dependsOn.forall { dependerName =>
      previousResults.find({ previousResult =>
        dependerName.equalsIgnoreCase(previousResult.getTestName)
      }).forall(_.testPassed)
    }
  }

  private def tryExecute(td: TestDefinition,
                         testOrder: Int,
                         plugin: Plugin): Try[PluginResult] = Try {
    val result: PluginResult = plugin.performAction(td.args, testOrder, td.name)
    if (td.writeArgs.isDefined) result.write(td.writeArgs.get)
    result.logResult()
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
