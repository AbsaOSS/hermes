package za.co.absa.hermes.datasetComparison.dataFrame

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SparkSession}

object Utils {
  private def setOptions(dfReader: DataFrameReader, parameters: Parameters): DataFrameReader =
    if (parameters.options.isEmpty) dfReader else dfReader.options(parameters.options)

  private def setOptions[T](dfReader: DataFrameWriter[T], parameters: Parameters): DataFrameWriter[T] =
    if (parameters.options.isEmpty) dfReader else dfReader.options(parameters.options)

  private def load(dfReader: DataFrameReader, parameters: Parameters): DataFrame =
    if (parameters.format == "jdbc") dfReader.load()
    else dfReader.load(parameters.path)

  private def save[T](dfWriter: DataFrameWriter[T], parameters: Parameters, endPath: String = ""): Unit =
    if (parameters.format == "jdbc") dfWriter.save()
    else dfWriter.save(endPath)

  def getUniqueFilePath(parameters: Parameters, extraPath: String, conf: Configuration): String = {
    val fs = FileSystem.get(conf)
    val basePath = s"${parameters.path}$extraPath"

    @scala.annotation.tailrec
    def appendNumberAndTest(namePt1: String,
                            namePt2: String,
                            condition: String => Boolean,
                            count: Int = 1): String = {
      val newName = s"${namePt1}_run$count$namePt2"
      if (condition(newName)) { appendNumberAndTest(namePt1, namePt2, condition, count + 1) }
      else newName
    }

    if (fs.exists(new Path(basePath))) {
      appendNumberAndTest(parameters.path, extraPath, { x: String => fs.exists(new Path(x)) })
    } else {
      basePath
    }
  }

  def loadDataFrame(parameters: Parameters)(implicit spark: SparkSession): DataFrame = {
    val dfReader = spark.read.format(parameters.format)
    val withOptions = setOptions(dfReader, parameters)
    load(withOptions, parameters)
  }

  def writeDataFrame(df: DataFrame, parameters: Parameters, pathSuffix: String = "")
                    (implicit spark: SparkSession): Unit = {
    val dfWriter =  df.write.format(parameters.format)
    val withOptions = setOptions(dfWriter, parameters)
    save(withOptions, parameters,s"${parameters.path}$pathSuffix")
  }

  def writeNextDataFrame(df: DataFrame, parameters: Parameters, pathSuffix: String = "")
                        (implicit spark: SparkSession): String = {
    val uniqueFilePath: String = getUniqueFilePath(parameters, pathSuffix, spark.sparkContext.hadoopConfiguration)
    val dfWriter =  df.write.format(parameters.format)
    val withOptions = setOptions(dfWriter, parameters)
    save(withOptions, parameters, uniqueFilePath)
    uniqueFilePath
  }
}
