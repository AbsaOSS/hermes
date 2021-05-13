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

import org.apache.spark.sql.{AccessShowString, DataFrame}
import za.co.absa.hermes.datasetComparison.config.ManualConfig

case class DataFrameMismatch(msg: String) extends Exception(msg)

object UnitTestDataFrame {

  /**
   * Assertion style metod for the usage of DatasetComparator in Unit Tests
   *
   * @param actualDF Actual DataFrame
   * @param expectedDF Expected DataFrame
   * @param allowDuplicates Should assertion fail if it encounters duplicities. Defaults to true.
   * @param keys Keys for usage as precise row identification. Defaults to an empty Set
   * @param numRows Number of rows to be printed in case of mismatch. Defaults to 20.
   */
  def assertDataFrame(actualDF: DataFrame,
                      expectedDF: DataFrame,
                      allowDuplicates: Boolean = true,
                      keys: Set[String] = Set.empty[String],
                      numRows: Int = 20): Unit = {
    val config = new ManualConfig(
      errorColumnName = "errCol",
      actualPrefix = "actual",
      expectedPrefix = "expected",
      allowDuplicates = allowDuplicates
    )
    val result = new DatasetComparator(expectedDF, actualDF, keys, config).compare

    if (!result.passed) {
      val message = AccessShowString.showString(result.resultDF.get, numRows)
      throw DataFrameMismatch(message)
    }

  }
}
