package za.co.absa.hermes.datasetComparison

import org.apache.spark.sql.{DataFrameReader, SparkSession}

object DataFrameReaderFactory {

  def getDFReaderFromCmdConfig(cmd: DatasetComparisonConfig)
                              (implicit sparkSession: SparkSession): DataFrameReader = {
    cmd.rawFormat match {
      case "csv" => getCsvReader(cmd)
      case "xml" => getXmlReader(cmd)
      case "parquet" => getParquetReader(cmd)
      case _ => getStandardReader(cmd)
    }
  }

  private def getStandardReader(dfReaderOptions: DatasetComparisonConfig)
                               (implicit sparkSession: SparkSession): DataFrameReader = {
    sparkSession.read.format(dfReaderOptions.rawFormat)
  }

  private def getParquetReader(dfReaderOptions: DatasetComparisonConfig)
                              (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions)
  }

  private def getXmlReader(dfReaderOptions: DatasetComparisonConfig)
                          (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions).option("rowTag", dfReaderOptions.rowTag.get)
  }

  private def getCsvReader(dfReaderOptions: DatasetComparisonConfig)
                          (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions)
      .option("delimiter", dfReaderOptions.csvDelimiter)
      .option("header", dfReaderOptions.csvHeader)
  }

}
