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

package za.co.absa.hermes.e2eRunner.plugins

import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import za.co.absa.hermes.datasetComparison.ComparisonResult
import za.co.absa.hermes.utils.SparkTestBase

class DatasetComparatorPluginTest extends FunSuite with BeforeAndAfterEach with SparkTestBase {

  private val plugin = new DatasetComparisonPlugin()

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
    additionalInfo = extraMap
  )

  private val result1 = DatasetComparisonResult(Array("Some", "arguments", "passed"), comparisonResult1, 10,
    "UnitTestComparisonResult", true, extraMap)

  private val format = new SimpleDateFormat("yyyy_MM_dd-HH_mm_ss")
  private var timePrefix = ""

  override def beforeEach(): Unit = {
    timePrefix = format.format(Calendar.getInstance().getTime)
  }

  test("Result - testWrite") {
    val outPath = s"target/test_output/e2e/comparison_plugin/$timePrefix"
    result1.write(Array("--out-path", outPath, "--ignores-this", "option"))
    assert(Files.exists(Paths.get(outPath,"_METRICS")))
  }

  test("Plugin - name") {
    assert(plugin.name == "DatasetComparison")
  }

  test("Plugin - performAction") {
    val shouldPass = true
    val order = 111
    val testName = "UnitTest"
    val args = Array(
      "--ref-path", getClass.getResource("/DatasetComparisonPlugin/dataset1.csv").toString,
      "--new-path", getClass.getResource("/DatasetComparisonPlugin/dataset1.csv").toString,
      "--format", "csv",
      "--header", "true"
    )

    val schemaSelector = List("id", "first_name", "last_name", "email", "gender", "ip_address").map(col)
    val passedOptions = "Omitted because of paths"
    val comparisonResult = ComparisonResult(9,9,0,0,9,schemaSelector,None,0,passedOptions,Map())
    val expectedDatasetComparisonResult = DatasetComparisonResult(args, comparisonResult, order, testName, shouldPass)

    val result = plugin.performAction(args, order, testName).asInstanceOf[DatasetComparisonResult]
    val resultOptionsOmitted = result.copy(returnedValue = result.returnedValue.copy(passedOptions = passedOptions))

    assert(expectedDatasetComparisonResult == resultOptionsOmitted)
  }
}
