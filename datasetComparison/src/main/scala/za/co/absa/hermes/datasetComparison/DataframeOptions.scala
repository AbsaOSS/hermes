package za.co.absa.hermes.datasetComparison

import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.sql._

case class DataframeOptions(format: String, options: Map[String, String], path: String) {

  private def setOptions(dfReader: DataFrameReader): DataFrameReader =
    if ( options.isEmpty ) dfReader else dfReader.options(options)

  private def load(dfReader: DataFrameReader): DataFrame =
    if (format == "jdbc") { dfReader.load()     }
    else                  { dfReader.load(path) }

  def loadDataFrame(implicit spark: SparkSession): DataFrame = {
    val general = spark.read.format(format)
    val withOptions = setOptions(general)
    load(withOptions)
  }
}

object DataframeOptions {
  def validateAndCreate(options: Map[String, String]): DataframeOptions = {
    val format = options.getOrElse("format", throw new MissingArgumentException(
      """Format is mandatory option. Use
        |"--format", "--ref-format" or "--new-format".""".stripMargin))
    val path = if (format == "jdbc") {
      options.getOrElse("dbtable", throw new MissingArgumentException(
        """DB table name is mandatory option for format
          |type jdbc. Use "--dbtable", "--ref-dbtable" or "--new-dbtable".""".stripMargin))
    } else {
      options.getOrElse("path", throw new MissingArgumentException(
        """Path is mandatory option for format type jdbc.
          |Use "--path", "--ref-path" or "--new-path".""".stripMargin))
    }
    val otherOptions = options -- Set("format", "path")
    DataframeOptions(format, otherOptions, path)
  }
}
