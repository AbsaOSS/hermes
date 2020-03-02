package za.co.absa.hermes.datasetComparison

import org.apache.spark.sql.Column
import org.scalatest.FunSuite

class ComparisonResultSuite extends FunSuite {

  test("testGetJsonMetadata") {
    val CR1 = ComparisonResult(10, 11, 12, List.empty[Column], None, 13, "--alfa beta")
    val result = """{
                   |  "passed":"false",
                   |  "numberOfDuplicates":"12",
                   |  "referenceRowCount":"10",
                   |  "newRowCount":"11",
                   |  "numberOfDifferences":"13",
                   |  "passedOptions":"--alfa beta"
                   |}""".stripMargin

    assert(result == CR1.getJsonMetadata)
  }

  test("testGetMetadata") {
    val CR2 = ComparisonResult(0, 0, 0, List.empty[Column], None, 0, "--alfa beta")
    val result = Map(
      "passed" -> "true",
      "numberOfDuplicates" -> "0",
      "referenceRowCount" -> "0",
      "newRowCount" -> "0",
      "numberOfDifferences" -> "0",
      "passedOptions" -> "--alfa beta"
    )

    assert(result == CR2.getMetadata)
  }

}
