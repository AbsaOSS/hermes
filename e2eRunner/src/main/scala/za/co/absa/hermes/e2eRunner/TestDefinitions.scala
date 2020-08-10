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

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.io.Source

object TestDefinitionJsonProtocol extends DefaultJsonProtocol {
  implicit val pluginDefinitionFormat: RootJsonFormat[TestDefinition] = jsonFormat6(TestDefinition.apply)
}

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

/**
 * A representation of a test definition together with it's actual order
 * @param definition TestDefinition instance
 * @param actualOrder Actual order of the test for running.
 */
case class TestDefinitionWithOrder(definition: TestDefinition, actualOrder: Int)

/**
 * Test definition representation.
 *
 * @param name Name of the test. This is for user's recognition
 * @param order Order of the test to be executed in. This is not an actual order, more like weight of the test.
 *              So you can specify something that must run last as 9999999 and something as first as -1.
 * @param pluginName User-friendly name of the plugin to be executed.
 * @param args Arguments to be provided into the performAction method of classes that implement the Plugin.
 * @param writeArgs Optional arguments for the write method implementations of PluginResult.
 */
case class TestDefinition(name: String,
                          order: Int,
                          pluginName: String,
                          args: Array[String],
                          dependsOn: Option[String],
                          writeArgs: Option[Array[String]])

object TestDefinition {
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
    if (maybeVars.isDefined) scribe.info("""Loaded "vars" key from test definitions""")
    else { scribe.info("""Could not find "vars" key in test definitions""") }

    val maybeRuns = parsedJson.asJsObject.getFields("runs").headOption
    val runsString = if (maybeRuns.isEmpty) { throw TestDefinitionJsonMalformed("Runs key not defined")}
    else { maybeRuns.get.toString() }

    val formattedJson = if (maybeVars.isDefined) {
      val varsMap = maybeVars.get.convertTo[Map[String, String]]
      val remappedJson = varsMap.foldLeft(runsString) { case (acc, (k,v)) => acc.replaceAllLiterally(s"#{$k}#", v) }
      remappedJson.parseJson
    } else {
      parsedJson
    }

    TestDefinitions(formattedJson.convertTo[Seq[TestDefinition]])
  }

  def fromFile(path: String): TestDefinitions = {
    val testDefinitionSource = Source.fromFile(path)
    val testDefinitionString = try testDefinitionSource.mkString finally { testDefinitionSource.close() }

    TestDefinition.fromString(testDefinitionString)
  }
}
