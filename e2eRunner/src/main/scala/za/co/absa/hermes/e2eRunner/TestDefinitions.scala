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

import scala.io.Source

case class TestDefinitions(private val testDefinitions: Seq[TestDefinition])  {
  def ensureOrderAndDependenciesCorrect(): Unit = {
    val output = getSorted.foldLeft((Seq.empty[String], Seq.empty[String])) { case (acc, td) =>
      if (td.dependsOn.forall(d => acc._2.contains(d))){
        (acc._1, acc._2 :+ td.name)
      } else {
        (acc._1 :+ td.name, acc._2 :+ td.name)
      }
    }

    if (output._1.nonEmpty) {
      throw TestDefinitionDependenciesOutOfOrder(output._1)
    }
  }

  /**
   * @return Returns the number of tests defined
   */
  def size: Int = testDefinitions.size

  /**
   * @return Returns a set of Plugin names used across test definitions
   */
  def getPluginNames: Set[String] = testDefinitions.map(_.pluginName).toSet

  /**
   * @return Returns sorted Sequence of TestDefinitions. Sorted by order and then by name.
   */
  def getSorted: Seq[TestDefinition] = testDefinitions.sortBy(pd => (pd.order, pd.pluginName))

  /**
   * @return Returns sorted TestDefinitionsWithIndex. Index here represents a run order.
   */
  def getSortedWithIndex: Seq[TestDefinitionWithOrder] = {
    getSorted
      .zipWithIndex
      .map({ case(td, index) => TestDefinitionWithOrder(td, index + 1)})
  }

  /**
   * Predefined foldLeft function that will always have an empty Sequence of Plugins as an accumulator
   * and a sorted TestDefinitions zipped with index to get actual run number.
   * @param op Operation to be executed
   * @return Returns a sequence of PluginResults
   */
  def foldLeftWithIndex(op: (Seq[PluginResult], TestDefinitionWithOrder) => Seq[PluginResult]): Seq[PluginResult] = {
    @scala.annotation.tailrec
    def fold(acc: Seq[PluginResult], seq: Seq[TestDefinitionWithOrder]): Seq[PluginResult] = seq match {
      case Seq()   => acc
      case head :: tail => fold(op(acc, head), tail)
    }
    fold(Seq.empty[PluginResult], getSortedWithIndex)
  }
}

object TestDefinitions {
  import TestDefinitionJsonProtocol._
  import spray.json._

  private def getVarsFromJson(parsedJson: JsValue): Map[String, String] = {
    parsedJson
      .asJsObject
      .getFields(TestDefinitionJsonProtocol.VarsField)
      .headOption
      .map(x => x.convertTo[Map[String, String]])
      .getOrElse(Map.empty)
  }

  private def getRunsFromJson(parsedJson: JsValue) = {
    parsedJson
      .asJsObject
      .getFields(TestDefinitionJsonProtocol.RunsField)
      .headOption
      .getOrElse(throw TestDefinitionJsonMalformed("Runs key not defined"))
      .toString
  }

  private def applyVars(vars: Map[String, String], runsString: String) = {
    val remappedJson = vars.foldLeft(runsString) { case (acc, (k, v)) => acc.replaceAllLiterally(s"#{$k}#", v) }
    val extraVars = TestDefinitionJsonProtocol.VarsPattern.findAllIn(remappedJson)
    if (extraVars.nonEmpty) throw UndefinedVariablesInTestDefinitionJson(extraVars.toSet)
    remappedJson
  }

  /**
   * Parses the TestDefinitions from json string.
   *
   * @param jsonString JsonString with test definitions
   * @return Returns a Sequence of TestDefinitions. These are not ordered or filtered in any way.
   */
  def fromString(jsonString: String, extraVars: Map[String, String] = Map.empty): TestDefinitions = {
    val parsedJson: JsValue = jsonString.parseJson
    val vars: Map[String, String] = getVarsFromJson(parsedJson) ++ extraVars
    val runsString: String = getRunsFromJson(parsedJson)
    val formattedJson = applyVars(vars, runsString)

    TestDefinitions(formattedJson.parseJson.convertTo[Seq[TestDefinition]])
  }

  def fromFile(path: String, extraVars: Map[String, String] = Map.empty): TestDefinitions = {
    val testDefinitionSource = Source.fromFile(path)
    val testDefinitionString = try testDefinitionSource.mkString finally { testDefinitionSource.close() }

    TestDefinitions.fromString(testDefinitionString, extraVars)
  }

  def fromSeq(testDefinitions: Seq[TestDefinition]): TestDefinitions = {
    TestDefinitions(testDefinitions)
  }
}
