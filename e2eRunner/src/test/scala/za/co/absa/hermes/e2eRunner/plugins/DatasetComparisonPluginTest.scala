package za.co.absa.hermes.e2eRunner.plugins

import org.apache.spark.sql.Column
import org.scalatest.FunSuite
import za.co.absa.hermes.datasetComparison.ComparisonResult
import za.co.absa.hermes.utils.SparkTestBase

class DatasetComparisonPluginTest extends FunSuite with SparkTestBase {

  private val extraMap = Map(
    "referenceOptions.path" -> "/cliOptions/referenceOptions/path",
    "newOptions.path" -> "cliOptions/newOptions/path",
    "outOptions.path" -> "cliOptions/outOptions/path"
  )

  private val comparisonResult1 = ComparisonResult(
    refRowCount = 100L,
    newRowCount = 100L,
    refDuplicateCount = 0L,
    newDuplicateCount = 0L,
    passedCount = 100L,
    usedSchemaSelector = List.empty[Column],
    resultDF = None,
    diffCount = 0,
    passedOptions = "",
    additionalInfo = Map.empty[String, String]
  )

  private val result1 = DatasetComparisonResult(Array("Some", "arguments", "passed"), comparisonResult1, 10,
    "UnitTestComparisonResult", true, extraMap)

  test("Result - testWrite") {
//    result1.write(Array.empty[String])
  }

}
