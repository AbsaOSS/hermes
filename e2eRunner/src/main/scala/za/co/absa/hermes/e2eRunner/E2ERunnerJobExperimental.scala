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

import scala.io.Source
import scala.util.{Failure, Success}

object E2ERunnerJobExperimental {
  def main(args: Array[String]): Unit = {
    val cmd = E2ERunnerConfigExperimental.getCmdLineArguments(args) match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }

    val thisJarPath = new File(this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getPath
    val classpath = List(Some(thisJarPath), cmd.jarPath).flatten.map(new File(_))
    scribe.info(
      s"""Plugin classes will be loaded from these paths:
         |${classpath.mkString("\n")}""".stripMargin)

    val source = Source.fromFile(cmd.testDefinition)
    val lines = try source.mkString finally { source.close() }

    val pluginManager: PluginManager = PluginManager(classpath)
    val pluginNames = pluginManager.getPluginNames
    scribe.info(
      s"""Loaded plugins are:
         |${pluginNames.mkString("\n")}""".stripMargin)

    val testDefinitions = TestDefinition.fromString(lines)
    val pluginExpectedToUse = testDefinitions.map(_.pluginName).toSet
    scribe.info(s"Loaded ${testDefinitions.size} test definitions")

    if (pluginExpectedToUse.diff(pluginNames).nonEmpty)
      throw NotAllRequestedPluginsDefined(pluginNames.diff(pluginExpectedToUse))

    scribe.info("Running tests")
    val results: Seq[PluginResult] = pluginManager.runWithDefinitions(testDefinitions)

    scribe.info("##################################################")
    scribe.info("Invoking logging of test results")
    scribe.info("##################################################")
    results.foreach({ x =>
      x.logResult()
      scribe.info("##################################################")
    })
  }
}
