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

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.spark.sql.SparkSession
import za.co.absa.hermes.datasetComparison.cliUtils.{CliOptions, DataframeOptions}
import za.co.absa.hermes.infoFileComparison.{InfoComparisonArguments, InfoFileComparisonJob, InfoFilesDifferException}
import za.co.absa.hermes.datasetComparison.{DatasetComparisonException, DatasetComparisonJob}
import za.co.absa.hermes.utils.HelperFunctions

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.sys.process._
import scala.util.{Failure, Success, Try}

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

  private val confExtraJavaOpts = conf.getObject("e2e-runner.sparkConf.extraJavaOptions")

  private val stdFullJarPath = s"$dceJarPath/$dceStdName"
  private val confFullJarPath = s"$dceJarPath/$dceConfName"

  private val stdParamsOverride = Try(conf.getString("e2e-runner.stdParams")).filter(_.nonEmpty).toOption
  private val confParamsOverride = Try(conf.getString("e2e-runner.confParams")).filter(_.nonEmpty).toOption

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

  private def getDatasetComparisonConfig(cmd: E2ERunnerConfig, pathHDFS: String): CliOptions = {
    val keys = if (cmd.keys.isDefined) { cmd.keys.get.toSet } else { Set.empty[String] }

    CliOptions(
      DataframeOptions("parquet", Map.empty[String, String], s"/ref$pathHDFS"),
      DataframeOptions("parquet", Map.empty[String, String], pathHDFS),
      s"/comp$pathHDFS",
      keys,
      "ran from E2E Runner"
    )
  }

  private def getInfoComparisonConfig(cmd: E2ERunnerConfig, pathHDFS: String): InfoComparisonArguments = {
    InfoComparisonArguments(
      newPath = s"$pathHDFS/_INFO",
      refPath = s"/ref$pathHDFS/_INFO",
      outPath = s"/comp/info_file$pathHDFS/_INFO"
    )
  }

  def main(args: Array[String]): Unit = {
    val cmd = E2ERunnerConfig.getCmdLineArguments(args, stdParamsOverride, confParamsOverride) match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }

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

    scribe.info("Standardization Comparison started")
    scribe.info("Standardization Comparison of Dataset")
    try {
      DatasetComparisonJob.execute(getDatasetComparisonConfig(cmd, stdPathHDFS))
    } catch {
      case e: DatasetComparisonException => errAccumulator.add(e.getMessage)
    }

    scribe.info("Standardization Comparison of INFO file")
    try {
      InfoFileComparisonJob.execute(getInfoComparisonConfig(cmd, stdPathHDFS))
    } catch {
      case e: InfoFilesDifferException => errAccumulator.add(e.getMessage)
    }

    scribe.info(s"Conformance started - $conformance")
    val (confTime, _) = HelperFunctions.calculateTime { runBashCmd(conformance) }
    scribe.info("Conformance Passed")

    scribe.info("Conformance Comparison started")
    scribe.info("Conformance Comparison of Dataset")
    try {
      DatasetComparisonJob.execute(getDatasetComparisonConfig(cmd, confPathHDFS))
    } catch {
      case e: DatasetComparisonException => errAccumulator.add(e.getMessage)
    }

    scribe.info("Conformance Comparison of INFO file")
    try {
      InfoFileComparisonJob.execute(getInfoComparisonConfig(cmd, confPathHDFS))
    } catch {
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
