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

import org.scalatest.{Assertions, FunSuite}
import za.co.absa.hermes.datasetComparison.cliUtils.CliParameters
import za.co.absa.hermes.datasetComparison.dataFrame.{Parameters, Utils}
import za.co.absa.hermes.utils.SparkTestBase
import za.co.absa.hermes.datasetComparison.UnitTestDataFrame._

class UnitTestDataFrameTest extends FunSuite with SparkTestBase {
  test("Test assertion") {
    val cliOptions = new CliParameters(
      Parameters("csv", Map("delimiter" -> ","), getClass.getResource("/dataSample5.csv").toString),
      Parameters("csv", Map("delimiter" -> ","), getClass.getResource("/dataSample6.csv").toString),
      Some(Parameters("parquet", Map.empty[String, String], "path/to/nowhere")),
      Set.empty[String],
      "--bogus raw-options"
    )

    val df1 = Utils.loadDataFrame(cliOptions.referenceDataParameters)
    val df2 = Utils.loadDataFrame(cliOptions.actualDataParameters)

    assertDataFrame(df1, df2, keys = Set("_c0"))
  }
}
