package za.co.absa.hermes.e2eRunner

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.spark.sql.SparkSession
import za.co.absa.hermes.datasetComparison.{DatasetComparisonConfig, DatasetComparisonJob, DatasetsDifferException}
import za.co.absa.hermes.infoFileComparison.{InfoComparisonConfig, InfoFileComparisonJob, InfoFilesDifferException}
import za.co.absa.hermes.utils.HelperFunctions

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.sys.process._

object E2ERunnerJob {
  private val conf: Config = ConfigFactory.load

  private val sparkExecutable = conf.getString("e2e-runner.sparkSubmitExecutable")

  private val dceJarPath = conf.getString("e2e-runner.dceJarPath")
  private val dceStdName = conf.getString("e2e-runner.dceStdName")
  private val dceConfName = conf.getString("e2e-runner.dceConfName")

  private val stdClass = conf.getString("e2e-runner.stdClass")
  private val confClass = conf.getString("e2e-runner.confClass")

  private val stdOutputPath = conf.getString("e2e-runner.stdPath")
  private val confOutputPath = conf.getString("e2e-runner.confPath")

  private val sparkSubmitOptions = conf.getString("e2e-runner.sparkSubmitOptions")

  private val confExtraJavaOpts: ConfigObject = conf.getObject("e2e-runner.sparkConf.extraJavaOptions")

  private val stdFullJarPath: String = s"$dceJarPath/$dceStdName"
  private val confFullJarPath: String = s"$dceJarPath/$dceConfName"

  private val stdParamsOverride: Option[String] = if (conf.hasPath("e2e-runner.stdParams") &&
                                                      conf.getString("e2e-runner.stdParams").nonEmpty) {
    Some(conf.getString("e2e-runner.stdParams"))
  } else {
    None
  }
  private val confParamsOverride: Option[String] = if (conf.hasPath("e2e-runner.confParams") &&
                                                       conf.getString("e2e-runner.confParams").nonEmpty) {
    Some(conf.getString("e2e-runner.confParams"))
  } else {
    None
  }

  private def runBashCmd(bashCmd: String): String = {
    (s"echo $bashCmd" #| "bash").!!
  }

  private def createSparkConfExtraJavaOptions(confObject: ConfigObject, stdPathHDFS: String): String = {
    val extraJavaConfPrefix = "--conf 'spark.driver.extraJavaOptions="
    val confWithoutLastApostrophe = confObject.unwrapped().foldLeft(extraJavaConfPrefix){ (acc, confOption) =>
      val (key, value) = confOption
      s"""$acc-D$key="${value}" """
    }

    s"""$confWithoutLastApostrophe -Dstandardized.hdfs.path="$stdPathHDFS"'"""
  }

  private def getDatasetComparisonConfig(cmd: E2ERunnerConfig, pathHDFS: String): DatasetComparisonConfig = {
    DatasetComparisonConfig(
      rawFormat = "parquet",
      rowTag = cmd.rowTag,
      csvDelimiter = cmd.csvDelimiter,
      csvHeader = cmd.csvHeader,
      newPath = pathHDFS,
      refPath = s"/ref$pathHDFS",
      outPath = s"/comp$pathHDFS",
      keys = cmd.keys
    )
  }

  private def getInfoComparisonConfig(cmd: E2ERunnerConfig, pathHDFS: String): InfoComparisonConfig = {
    InfoComparisonConfig(
      newPath = s"$pathHDFS/_INFO",
      refPath = s"/ref$pathHDFS/_INFO",
      outPath = s"/comp/info_file$pathHDFS/_INFO"
    )
  }

  def main(args: Array[String]): Unit = {
    val cmd = E2ERunnerConfig.getCmdLineArguments(args)

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(s"End2End test of ${cmd.datasetName} (v.${cmd.datasetVersion}) " +
        s"${cmd.reportVersion} v.${cmd.reportVersion}")
      .getOrCreate()

    val stdPathHDFS = HelperFunctions.richFormat(stdOutputPath, cmd.getDatasetMap)
    val confPathHDFS = HelperFunctions.richFormat(confOutputPath, cmd.getDatasetMap)

    val stdParams = if (stdParamsOverride.isDefined) stdParamsOverride.get else cmd.getStdParams
    val confParams = if (confParamsOverride.isDefined) confParamsOverride.get else cmd.getConfParams

    val extraJavaOpts = createSparkConfExtraJavaOptions(confExtraJavaOpts, stdPathHDFS)

    val sparkJobPrefix = s"$sparkExecutable $sparkSubmitOptions --class"
    val standardization = s"$sparkJobPrefix $stdClass $extraJavaOpts $stdFullJarPath $stdParams"
    val conformance = s"$sparkJobPrefix $confClass $extraJavaOpts $confFullJarPath $confParams"

    val errAccumulator = new ListBuffer[String]()

    scribe.info(s"Standardization started - $standardization")
    val (stdTime, _) = HelperFunctions.calculateTime { runBashCmd(standardization) }
    scribe.info("Standardization Passed")

    try {
      scribe.info("Standardization Comparison started")
      scribe.info("Standardization Comparison of Dataset")
      DatasetComparisonJob.execute(getDatasetComparisonConfig(cmd, stdPathHDFS))
      scribe.info("Standardization Comparison of INFO file")
      InfoFileComparisonJob.execute(getInfoComparisonConfig(cmd, stdPathHDFS))
    } catch {
      case e: DatasetsDifferException => errAccumulator.add(e.getMessage)
      case e: InfoFilesDifferException => errAccumulator.add(e.getMessage)
    }

    scribe.info(s"Conformance started - $conformance")
    val (confTime, _) = HelperFunctions.calculateTime { runBashCmd(conformance) }
    scribe.info("Conformance Passed")

    try {
      scribe.info("Conformance Comparison started")
      scribe.info("Conformance Comparison of Dataset")
      DatasetComparisonJob.execute(getDatasetComparisonConfig(cmd, confPathHDFS))
      scribe.info("Conformance Comparison of INFO file")
      InfoFileComparisonJob.execute(getInfoComparisonConfig(cmd, confPathHDFS))
    } catch {
      case e: DatasetsDifferException => errAccumulator.add(e.getMessage)
      case e: InfoFilesDifferException => errAccumulator.add(e.getMessage)
    }

    val humanReadableStdTime = HelperFunctions.prettyPrintElapsedTime(stdTime)
    val humanReadableConfTime = HelperFunctions.prettyPrintElapsedTime(confTime)
    scribe.info(s"Standardization and Conformance passed. It took them $humanReadableStdTime and " +
      s"$humanReadableConfTime respectively")

    if (errAccumulator.nonEmpty) {
      throw E2EComparisonException(errAccumulator.mkString("\n"))
    } else {
      scribe.info("All comparisons have passed")
    }

  }
}
