package za.co.absa.hermes.datasetComparison

import org.scalatest.FunSuite

class FormatConfigSuite extends FunSuite {

  private val newPath = "/tmp/standardized_out"
  private val refPath = "/tmp/reference_data"
  private val outPath = "/tmp/out_data"
  private val delimiter = ";"
  private val rowTag = "Alfa"
  private val csvFormat = "csv"
  private val parquetFormat = "parquet"
  private val xmlFormat = "xml"
  private val genParqDatasetConfig = new DatasetComparisonConfig(
    rawFormat = parquetFormat,
    refPath = refPath,
    newPath = newPath,
    outPath = outPath
  )
  private val genCsvDatasetConfig = new DatasetComparisonConfig(
    rawFormat = csvFormat,
    csvHeader = true,
    csvDelimiter = delimiter,
    refPath = refPath,
    newPath = newPath,
    outPath = outPath
  )
  private val genXmlDatasetConfig = new DatasetComparisonConfig(
    rawFormat = xmlFormat,
    rowTag = Option(rowTag),
    refPath = refPath,
    newPath = newPath,
    outPath = outPath
  )
  private val setCsvDatasetsConfig = new DatasetComparisonConfig(
    refRawFormat = csvFormat,
    refCsvHeader = true,
    refCsvDelimiter = delimiter,
    newRawFormat = csvFormat,
    newCsvHeader = false,
    newCsvDelimiter = ":",
    refPath = refPath,
    newPath = newPath,
    outPath = outPath
  )
  private val setEachDatasetConfig = new DatasetComparisonConfig(
    rawFormat = parquetFormat,
    csvHeader = true,
    refRawFormat = csvFormat,
    refCsvDelimiter = delimiter,
    newRawFormat = xmlFormat,
    newRowTag = Option(rowTag),
    refPath = refPath,
    newPath = newPath,
    outPath = outPath
  )

  test("Get config for generic parquet format") {
    val actualConfig: FormatConfig = FormatConfig.getComparisonArguments("refRawFormat", genParqDatasetConfig)
    val expectedConfig: FormatConfig = FormatConfig("parquet")

    assert (actualConfig == expectedConfig)

  }

  test("Get config for generic Csv format with parameters") {
    val actualConfig: FormatConfig = FormatConfig.getComparisonArguments("refRawFormat", genCsvDatasetConfig)
    val expectedConfig: FormatConfig = FormatConfig("csv", csvDelimiter = ";", csvHeader = true)

    assert (actualConfig == expectedConfig)

  }

  test("Get config for generic Xml format with parameters") {
    val actualConfig: FormatConfig = FormatConfig.getComparisonArguments("newRawFormat", genXmlDatasetConfig)
    val expectedConfig: FormatConfig = FormatConfig("xml", Option("Alfa"))

    assert (actualConfig == expectedConfig)

  }

  test("Get config for new and ref csv formats") {
    val actualRefConfig: FormatConfig = FormatConfig.getComparisonArguments("refRawFormat", setCsvDatasetsConfig)
    val expectedRefConfig: FormatConfig = FormatConfig("csv", csvDelimiter = ";", csvHeader = true)
    val actualNewConfig: FormatConfig = FormatConfig.getComparisonArguments("newRawFormat", setCsvDatasetsConfig)
    val expectedNewConfig: FormatConfig = FormatConfig("csv", csvDelimiter = ":", csvHeader = false)

    assert (actualRefConfig == expectedRefConfig)
    assert (actualNewConfig == expectedNewConfig)

  }

  test("Get config for each format") {
    val actualRefConfig: FormatConfig = FormatConfig.getComparisonArguments("refRawFormat", setEachDatasetConfig)
    val expectedRefConfig: FormatConfig = FormatConfig("csv", csvDelimiter = ";", csvHeader = false)
    val actualNewConfig: FormatConfig = FormatConfig.getComparisonArguments("newRawFormat", setEachDatasetConfig)
    val expectedNewConfig: FormatConfig = FormatConfig("xml", rowTag = Option("Alfa"))

    assert (actualRefConfig == expectedRefConfig)
    assert (actualNewConfig == expectedNewConfig)

  }
}
