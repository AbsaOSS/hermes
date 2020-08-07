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

import org.clapper.classutil.ScalaCompat.LazyList
import org.clapper.classutil.{ClassFinder, ClassInfo}

import scala.util.{Failure, Success, Try}

class PluginManager(private val plugins: Map[String, String]) {
  def getPlugin(name: String): Plugin = {
    val className = if (plugins.keySet.contains(name)) {
      plugins(name)
    } else throw PluginNotFound(name)

    Class.forName(className).newInstance().asInstanceOf[Plugin]
  }

  def getPluginNames: Set[String] = plugins.keySet

  def runWithDefinitions(testDefinitions: Seq[TestDefinition], failFast: Boolean = false): Seq[PluginResult] = {
    val sortedPD = testDefinitions.sortBy(pd => (pd.order, pd.pluginName))
    sortedPD.zipWithIndex.foldLeft(Seq.empty[PluginResult]) {
      case (acc, (td, i)) =>
        scribe.info(s"Running ${td.name}")
        val plugin: Plugin = getPlugin(td.pluginName)

        val tryExecution = if (canTestProceed(td, acc)) {
          tryExecute(td, i, plugin)
        } else {
          Failure(throw DependeeFailed(td.name, td.dependsOn.get))
        }

        tryExecution match {
          case Success(value) => acc :+ value
          case Failure(exception) if !failFast =>  acc :+ FailedPluginResult(td.args, exception, i, td.name)
          case Failure(exception) if failFast =>  throw exception
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
}

object PluginManager {
  def apply(plugins: Map[String, String]): PluginManager = new PluginManager(plugins)

  def apply(classpath: Seq[File] = Seq(new File("."))): PluginManager = {
    val plugins = getPluginsIterator(classpath)
    val pluginMap: Map[String, String] = getPluginsMap(plugins)
    PluginManager(pluginMap)
  }

  private def getPluginsMap(plugins: Iterator[ClassInfo]): Map[String, String] = {
    plugins.foldLeft(Map.empty[String, String]) {
      (acc, value) =>
        val plugin: Plugin = Class.forName(value.name).newInstance().asInstanceOf[Plugin]
        if (acc.keySet.contains(plugin.name)) throw DuplicatePluginNames(plugin.name)
        acc + (plugin.name -> value.name)
    }
  }

  private def getPluginsIterator(classpath: Seq[File]): Iterator[ClassInfo] = {
    val finder: ClassFinder = ClassFinder(classpath)
    val classes: LazyList[ClassInfo] = finder.getClasses
    val classMap = ClassFinder.classInfoMap(classes)
    val plugins: Iterator[ClassInfo] = ClassFinder.concreteSubclasses("za.co.absa.hermes.e2eRunner.Plugin", classMap)
    plugins
  }
}
