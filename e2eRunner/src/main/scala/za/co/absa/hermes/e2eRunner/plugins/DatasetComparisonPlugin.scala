package za.co.absa.hermes.e2eRunner.plugins

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import za.co.absa.hermes.datasetComparison.DatasetComparisonJob.{getConfig, getSchema}
import za.co.absa.hermes.datasetComparison.cliUtils.{CliOptions, CliOptionsParser, DataframeOptions}
import za.co.absa.hermes.datasetComparison.{ComparisonResult, DatasetComparison, DatasetComparisonJob}
import za.co.absa.hermes.e2eRunner.{Plugin, PluginResult}
import za.co.absa.hermes.utils.HelperFunctions

case class DatasetComparisonResult(arguments: Array[String],
                                   returnedValue: ComparisonResult,
                                   order: Int,
                                   testName: String,
                                   passed: Boolean,
                                   additionalInfo: Map[String, String] = Map.empty)
  extends PluginResult(arguments, returnedValue, order, testName, passed, additionalInfo) {

  /**
   * This method should be used to write the plugin result in a form required.
   *
   * @param writeArgs Arguments provided from the "writeArgs" key from the test definition json
   */
  override def write(writeArgs: Array[String]): Unit = {
    def sparkSession(name: String = "DatasetComparisonPlugin", sparkConf: Option[SparkConf] = None ): SparkSession = {
      val session = SparkSession.builder().appName(name)
      val withConf = if (sparkConf.isDefined) session.config(sparkConf.get) else session
      withConf.getOrCreate()
    }

    implicit val spark: SparkSession = sparkSession()
    val outDFOptions: DataframeOptions = CliOptionsParser.parseOutputParameters(writeArgs)

    returnedValue.resultDF match {
      case Some(df) => outDFOptions.writeDataFrame(df)
      case None => scribe.info(
        s"DatastComparison run as ${HelperFunctions.appendOrdinalSuffix(order)} had no difference, no DF written."
      )
    }

    DatasetComparisonJob.writeMetricsToFile(returnedValue, outDFOptions.path)
  }

  /**
   * Logs the result of the plugin execution at the end.
   */
  override def logResult(): Unit = {
    if (passed) {
      scribe.info(
        s"""Test $testName ($order) finished. Expected and actual data sets are the same. Metrics written to
           |${additionalInfo("outOptions.path")}/_METRICS""".stripMargin.replaceAll("[\\r\\n]", ""))
    } else {
      scribe.warn(s"""Test $testName ($order) finished. Expected and actual datasets differ.
                     |Reference path: ${additionalInfo("referenceOptions.path")}
                     |Actual dataset path: ${additionalInfo("newOptions.path")}
                     |Difference written to: ${additionalInfo("outOptions.path")}
                     |Count Expected( ${returnedValue.refRowCount} ) vs Actual( ${returnedValue.newRowCount} )""".stripMargin)

    }
  }
}

class DatasetComparisonPlugin extends Plugin {
  override def name: String = "DatasetComparison"

  override def performAction(args: Array[String], actualOrder: Int, testName: String): PluginResult = {
    def sparkSession(name: String = "DatasetComparisonPlugin", sparkConf: Option[SparkConf] = None ): SparkSession = {
      val session = SparkSession.builder().appName(name)
      val withConf = if (sparkConf.isDefined) session.config(sparkConf.get) else session
      withConf.getOrCreate()
    }

    implicit val spark: SparkSession = sparkSession()
    val cliOptions = CliOptionsParser.parseInputParameters(args)
    val optionalSchema = getSchema(cliOptions.schemaPath)
    val dataFrameRef = cliOptions.referenceOptions.loadDataFrame
    val dataFrameActual = cliOptions.actualOptions.loadDataFrame
    val dsComparison = new DatasetComparison(
      dataFrameRef,
      dataFrameActual,
      cliOptions.keys,
      optionalSchema = optionalSchema
    )

    val datasetResult: ComparisonResult = dsComparison.compare
    val datasetResultWithOptions = datasetResult.copy(passedOptions = args.mkString(" "))
    DatasetComparisonResult(args, datasetResultWithOptions, actualOrder, testName, datasetResultWithOptions.passed)
  }
}
