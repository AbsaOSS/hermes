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

case class TestDefinitionJsonMalformed(msg: String, cause: Throwable = None.orNull) extends Exception(msg, cause)

case class PluginNotFound(pluginName: String, cause: Throwable = None.orNull)
  extends Exception(s"Plugin $pluginName not found on the class path.", cause)

case class DependeeFailed(name: String, dependeeName: String)
  extends Exception(s"Test named $dependeeName failed, but it was destined as a dependee of $name")

case class DuplicatePluginNames(name: String)
  extends Exception(s"Found two or more plugins with same name $name")

case class TestFailedWithFailFastOn(name: String, index: Int, cause: Throwable = None.orNull)
  extends Exception(s"Test $name ($index) Failed. FailFast is turned on.", cause)

case class TestDefinitionDependenciesOutOfOrder(dependencies: Seq[String])
  extends Exception(
    s"""These test dependencies are out of order:
       |${dependencies.mkString("\n")}""".stripMargin)

case class UndefinedVariablesInTestDefinitionJson(vars: Set[String])
  extends Exception(s"""These vars were found in runs object and with no corresponding values:
                       |${vars.map(x => s"""- "$x"""").mkString("\n")}""".stripMargin)
