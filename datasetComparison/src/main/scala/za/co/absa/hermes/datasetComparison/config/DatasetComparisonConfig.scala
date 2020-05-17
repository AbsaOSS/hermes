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

package za.co.absa.hermes.datasetComparison.config

import scala.util.{Failure, Success, Try}

abstract class DatasetComparisonConfig {
  val errorColumnName: String
  val actualPrefix: String
  val expectedPrefix: String
  val allowDuplicates: Boolean

  def validate(): Try[DatasetComparisonConfig]  = {

    def validateColumnName(column_name: String, config_name: String): Try[Boolean] = {
      if (column_name.matches(".*[ ,;{}()\n\t=].*")) {
        Failure(new IllegalArgumentException(
          s"$config_name configuration options has forbidden characters for a column name"
        ))
      }
      else {
        Success(true)
      }
    }

    for {
      _errColumnName <- validateColumnName(errorColumnName, "errorColumnName")
      _actualPrefix <- validateColumnName(actualPrefix, "actualPrefix")
      _expectedPrefix <- validateColumnName(expectedPrefix, "expectedPrefix")
    } yield this
  }
}
