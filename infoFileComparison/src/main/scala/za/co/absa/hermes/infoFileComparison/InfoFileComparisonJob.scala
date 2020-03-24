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

package za.co.absa.hermes.infoFileComparison

import java.io.FileInputStream

import better.files.File
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.ControlMeasuresParser
import za.co.absa.atum.utils.ARMImplicits
import za.co.absa.hermes.infoFileComparison.AtumModelUtils._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

object InfoFileComparisonJob {
  private val conf: Config = ConfigFactory.load
  private val bufferSizeDefaultValue = conf.getInt("info-file-comparison.comparison-job.bufferSizeDefaultValue")

  private lazy val hadoopConfiguration = getHadoopConfiguration

  def main(args: Array[String]): Unit = {
    val cmd = InfoComparisonArguments.getCmdLineArguments(args) match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }

    execute(cmd)
  }

  /**
    * Execute the comparison
    *
    * @param cmd Provided configuration for the comparison
    * @param configPath Path to TypeSafe's style config file
    */
  def execute(cmd: InfoComparisonArguments, configPath: Option[String] = None): Unit = {
    val newControlMeasure = loadControlMeasures(cmd.newPath)
    val refControlMeasure = loadControlMeasures(cmd.refPath)
    val config = InfoFileComparisonConfig.fromTypesafeConfig(configPath)

    val diff: List[ModelDifference[_]] = compare(newControlMeasure, refControlMeasure, config)

    if (diff.nonEmpty) {
      val serializedData = ModelDifferenceParser.asJson(diff)
      saveDataToFile(serializedData, cmd.outPath)

      throw InfoFilesDifferException(cmd.refPath, cmd.newPath, cmd.outPath)
    } else {
      scribe.info("Expected and actual _INFO files are the same.")
    }
  }

  private def compare(newControlMeasure: ControlMeasure,
                      refControlMeasure: ControlMeasure,
                      config: InfoFileComparisonConfig): List[ModelDifference[_]] = {
    refControlMeasure.compareWith(newControlMeasure, config)
  }

  private def saveDataToFile(data: String, path: String): Unit = {
    path match {
      case p if p.startsWith("file://") =>
        File(p.stripPrefix("file://")).createIfNotExists(createParents = true).write(data)
      case p                            =>
        saveDataToHDFSFile(data, new Path(p))
    }
  }

  private def saveDataToHDFSFile(data: String, path: Path): Unit = {
    import ARMImplicits._

    val fs = FileSystem.get(hadoopConfiguration)
    val overwrite = true
    val progress = null // scalastyle:ignore null
    val permission = new FsPermission("777")
    val bufferSize = hadoopConfiguration.getInt("io.file.buffer.size", bufferSizeDefaultValue)

    for (fos <- fs.create(
            path,
            permission,
            overwrite,
            bufferSize,
            fs.getDefaultReplication(path),
            fs.getDefaultBlockSize(path),
            progress)
         ){
      fos.write(data.getBytes)
    }
  }

  private def getHadoopConfiguration: Configuration = {
    val hadoopConfDir = sys.env("HADOOP_CONF_DIR")
    val coreSiteXmlPath = s"$hadoopConfDir/core-site.xml"
    val hdfsSiteXmlPath = s"$hadoopConfDir/hdfs-site.xml"
    val conf = new Configuration()
    conf.clear()

    conf.addResource(new Path(coreSiteXmlPath))
    conf.addResource(new Path(hdfsSiteXmlPath))
    conf
  }

  private def loadControlMeasures(path: String): ControlMeasure = {
    val stream = path match {
      case p if p.startsWith("file://") => new FileInputStream(p.stripPrefix("file://"))
      case p                            => FileSystem.get(hadoopConfiguration).open(new Path(p))
    }
    val controlInfoJson = try IOUtils.readLines(stream).asScala.mkString("\n") finally stream.close()
    ControlMeasuresParser.fromJson(controlInfoJson)
  }
}
