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

object InfoFileComparisonJob {
  private val conf: Config = ConfigFactory.load
  private val bufferSizeDefaultValue = conf.getInt("info-file-comparison.comparison-job.bufferSizeDefaultValue")

  private lazy val hadoopConfiguration = getHadoopConfiguration

  def main(args: Array[String]): Unit = {
    val cmd = InfoComparisonConfig.getCmdLineArguments(args)

    execute(cmd)
  }

  /**
    * Execute the comparison
    *
    * @param cmd Provided configuration for the comparison
    */
  def execute(cmd: InfoComparisonConfig): Unit = {
    val newControlMeasure = loadControlMeasures(cmd.newPath)
    val refControlMeasure = loadControlMeasures(cmd.refPath)

    val diff: List[ModelDifference[_]] = refControlMeasure.compareWith(newControlMeasure)

    if (diff.nonEmpty) {
      val serializedData = ModelDifferenceParser.asJson(diff)
      saveDataToFile(serializedData, cmd.outPath)

      throw InfoFilesDifferException(cmd.refPath, cmd.newPath, cmd.outPath)
    } else {
      scribe.info("Expected and actual _INFO files are the same.")
    }
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
