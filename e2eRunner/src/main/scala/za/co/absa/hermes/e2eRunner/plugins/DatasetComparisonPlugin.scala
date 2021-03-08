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

package za.co.absa.hermes.e2eRunner.plugins

import org.apache.spark.sql.SparkSession
import za.co.absa.hermes.datasetComparison.cliUtils.CliParametersParser
import za.co.absa.hermes.datasetComparison.dataFrame.{Parameters, Utils}
import za.co.absa.hermes.datasetComparison.{ComparisonResult, DatasetComparator, DatasetComparisonJob}
import za.co.absa.hermes.e2eRunner.logging.{ErrorResultLog, InfoResultLog, ResultLog}
import za.co.absa.hermes.e2eRunner.{Plugin, PluginResult, SparkBase, TestDefinition}
import za.co.absa.hermes.utils.HelperFunctions

case class DatasetComparisonResult(arguments: Array[String],
                                   returnedValue: ComparisonResult,
                                   order: Int,
                                   testName: String,
                                   passed: Boolean,
                                   additionalInfo: Map[String, String] = Map.empty) extends PluginResult with SparkBase {

  /**
   * This method should be used to write the plugin result in a form required.
   *
   * @param writeArgs Arguments provided from the "writeArgs" key from the test definition json
   */
  override def write(writeArgs: Array[String]): Unit = {
    implicit val spark: SparkSession = sparkSession("DatasetComparisonPlugin")
    val outDFOptions: Parameters = CliParametersParser.parseOutputParameters(writeArgs)

    returnedValue.resultDF match {
      case Some(df) => Utils.writeDataFrame(df, outDFOptions)
      case None => scribe.info(
        s"DatastComparison run as ${HelperFunctions.appendOrdinalSuffix(order)} had no difference, no DF written."
      )
    }

    DatasetComparisonJob.writeMetricsToFile(returnedValue, outDFOptions.path)
  }

  /**
   * Logs the result of the plugin execution at the end.
   */
  override def resultLog: ResultLog = {
    if (passed) {
      InfoResultLog(
        s"""Test $testName ($order) finished. Expected and actual data sets are the same. Metrics written to
         | ${additionalInfo("outOptions.path")}/_METRICS""".stripMargin.replaceAll("[\\r\\n]", "")
      )
    } else {
      ErrorResultLog(
        s"""Test $testName ($order) finished. Expected and actual datasets differ.
         |Reference path: ${additionalInfo("referenceOptions.path")}
         |Actual dataset path: ${additionalInfo("newOptions.path")}
         |Difference written to: ${additionalInfo("outOptions.path")}
         |Count Expected( ${returnedValue.refRowCount} ) vs Actual( ${returnedValue.newRowCount} )""".stripMargin
      )
    }
  }
}

class DatasetComparisonPlugin extends Plugin with SparkBase {
  override def name: String = "DatasetComparison"

  override def performAction(testDefinition: TestDefinition, actualOrder: Int): DatasetComparisonResult = {
    val args = testDefinition.args
    val testName = testDefinition.name

    implicit val spark: SparkSession = sparkSession("DatasetComparisonPlugin")
    val cliOptions = CliParametersParser.parseInputParameters(args)
    val outOptions = CliParametersParser.parseOutputParameters(testDefinition.writeArgs.getOrElse(Array.empty))
    val optionalSchema = DatasetComparisonJob.getSchema(cliOptions.schemaPath)
    val dataFrameRef = Utils.loadDataFrame(cliOptions.referenceDataParameters)
    val dataFrameActual = Utils.loadDataFrame(cliOptions.actualDataParameters)
    val dsComparison = new DatasetComparator(
      dataFrameRef,
      dataFrameActual,
      cliOptions.keys,
      optionalSchema = optionalSchema
    )

    val datasetResult: ComparisonResult = dsComparison.compare

    val additionalInfo = Map(
      "referenceOptions.path" -> cliOptions.referenceDataParameters.path,
      "newOptions.path" -> cliOptions.actualDataParameters.path,
      "outOptions.path" -> outOptions.path
    )
    val datasetResultWithOptions = datasetResult.copy(passedOptions = args.mkString(" "))
    DatasetComparisonResult(args, datasetResultWithOptions, actualOrder, testName, datasetResultWithOptions.passed, additionalInfo)
  }
}
