/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hermes.datasetComparison

import org.apache.spark.sql.Column
import org.scalatest.FunSuite

class ComparisonResultSuite extends FunSuite {

  test("testGetJsonMetadata") {
    val CR1 = ComparisonResult(10, 11, 14, 15, 12, List.empty[Column], None, 13, "--alfa beta")
    val result = """{
                   |  "additionalInfo": {
                   |
                   |  },
                   |  "newDuplicateCount": 15,
                   |  "newRowCount": 11,
                   |  "numberOfDifferences": 13,
                   |  "passed": false,
                   |  "passedOptions": "--alfa beta",
                   |  "passedRowsCount": 12,
                   |  "refDuplicateCount": 14,
                   |  "referenceRowCount": 10
                   |}""".stripMargin

    assert(result == CR1.getPrettyJson)
  }

  test("testGetMetadata") {
    val CR2 = ComparisonResult(0, 0, 0, 0, 0, List.empty[Column], None, 0, "--alfa beta")
    val result = Map(
      "passed" -> true,
      "refDuplicateCount" -> 0,
      "referenceRowCount" -> 0,
      "newRowCount" -> 0,
      "newDuplicateCount" -> 0,
      "additionalInfo" -> Map(),
      "numberOfDifferences" -> 0,
      "passedRowsCount" -> 0,
      "passedOptions" -> "--alfa beta"
    )

    assert(result == CR2.getMetadata)
  }

}
