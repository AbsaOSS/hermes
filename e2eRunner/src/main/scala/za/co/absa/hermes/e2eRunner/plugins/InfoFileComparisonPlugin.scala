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

package za.co.absa.hermes.e2eRunner.plugins

import za.co.absa.hermes.e2eRunner.logging.{ErrorResultLog, InfoResultLog, ResultLog}
import za.co.absa.hermes.e2eRunner.{Plugin, PluginResult, TestDefinition}
import za.co.absa.hermes.infoFileComparison.AtumModelUtils._
import za.co.absa.hermes.infoFileComparison.InfoFileComparisonJob
import za.co.absa.hermes.infoFileComparison._

import scala.util.{Failure, Success}

case class InfoFileComparisonResult(arguments: Array[String],
                                    returnedValue: List[ModelDifference[_]],
                                    order: Int,
                                    testName: String,
                                    passed: Boolean,
                                    additionalInfo: Map[String, String])
  extends PluginResult(arguments, returnedValue, order, testName, passed, additionalInfo) {

  /**
   * This method should be used to write the plugin result in a form required.
   *
   * @param writeArgs Arguments provided from the "writeArgs" key from the test definition json
   */
  override def write(writeArgs: Array[String]): Unit = {
    if (!passed) {
      val serializedData = ModelDifferenceParser.asJson(returnedValue)
      InfoFileComparisonJob.saveDataToFile(serializedData, additionalInfo("outPath"))
    }
  }

  /**
   * Logs the result of the plugin execution at the end.
   */
  override def resultLog: ResultLog = {
    if (passed) {
      InfoResultLog(s"Test $testName ($order) finished. Expected and actual _INFO files are the same.")
    } else {
      ErrorResultLog(
        s"""Test $testName ($order) finished. Expected and actual info files differ.
          |Reference path: ${additionalInfo("refPath")}
          |Actual dataset path: ${additionalInfo("newPath")}
          |Difference written to: ${additionalInfo("outPath")}""".stripMargin
      )
    }
  }
}

class InfoFileComparisonPlugin extends Plugin {
  override def name: String = "InfoFileComparison"

  override def performAction(testDefinition: TestDefinition, actualOrder: Int): InfoFileComparisonResult = {
    val args = testDefinition.args
    val testName = testDefinition.name

    val parsedArgs = InfoComparisonArguments.getCmdLineArguments(args) match {
      case Success(value) => value.toStringMap
      case Failure(exception)  => throw exception
    }

    val newControlMeasure = InfoFileComparisonJob.loadControlMeasures(parsedArgs("newPath"))
    val refControlMeasure = InfoFileComparisonJob.loadControlMeasures(parsedArgs("refPath"))
    val config = InfoFileComparisonConfig.fromTypesafeConfig()

    val diff: List[ModelDifference[_]] = refControlMeasure.compareWith(newControlMeasure, config)

    InfoFileComparisonResult(args, diff, actualOrder, testName, diff.isEmpty, parsedArgs)
  }
}
