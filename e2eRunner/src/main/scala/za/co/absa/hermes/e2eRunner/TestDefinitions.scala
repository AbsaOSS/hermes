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

  /**
   * Parses the TestDefinitions from json string.
   *
   * @param jsonString JsonString with test definitions
   * @return Returns a Sequence of TestDefinitions. These are not ordered or filtered in any way.
   */
  def fromString(jsonString: String): TestDefinitions = {
    val parsedJson: JsValue = jsonString.parseJson
    val maybeVars: Option[JsValue] = parsedJson.asJsObject.getFields("vars").headOption

    val maybeRuns = parsedJson.asJsObject.getFields("runs").headOption
    val runsString = if (maybeRuns.isEmpty) { throw TestDefinitionJsonMalformed("Runs key not defined")}
    else { maybeRuns.get.toString() }

    val formattedJson = if (maybeVars.isDefined) {
      scribe.info("""Loaded "vars" key from test definitions""")
      val varsMap = maybeVars.get.convertTo[Map[String, String]]
      val remappedJson = varsMap.foldLeft(runsString) { case (acc, (k,v)) => acc.replaceAllLiterally(s"#{$k}#", v) }
      remappedJson.parseJson
    } else {
      scribe.info("""Could not find "vars" key in test definitions""")
      parsedJson
    }

    TestDefinitions(formattedJson.convertTo[Seq[TestDefinition]])
  }

  def fromFile(path: String): TestDefinitions = {
    val testDefinitionSource = Source.fromFile(path)
    val testDefinitionString = try testDefinitionSource.mkString finally { testDefinitionSource.close() }

    TestDefinitions.fromString(testDefinitionString)
  }
}
