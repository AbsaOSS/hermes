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

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.{Failure, Success, Try}

case class DatasetComparisonConfig(errorColumnName: String,
                                   actualPrefix: String,
                                   expectedPrefix: String,
                                   allowDuplicates: Boolean) {

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
      _ <- validateColumnName(errorColumnName, "errorColumnName")
      _ <- validateColumnName(actualPrefix, "actualPrefix")
      _ <- validateColumnName(expectedPrefix, "expectedPrefix")
    } yield this
  }

  def getLoggableString: String = {
    s"""Effective DatasetComparison configuration:
       | Error Column Name (errorColumnName) -> "$errorColumnName"
       | Prefix of original columns (expectedPrefix) -> "$expectedPrefix"
       | Prefix of new columns (actualPrefix) -> "$actualPrefix"
       | Allow duplicities in dataframes (allowDuplicates) -> "$allowDuplicates"""".stripMargin
  }
}

object DatasetComparisonConfig {
  def fromTypeSafeConfig(path: Option[String] = None): DatasetComparisonConfig = {
    val conf: Config = path match {
      case Some(x) => ConfigFactory.load(x)
      case None    => ConfigFactory.load()
    }

    val errorColumnName: String = conf.getString("dataset-comparison.errColumn")
    val actualPrefix: String = conf.getString("dataset-comparison.actualPrefix")
    val expectedPrefix: String = conf.getString("dataset-comparison.expectedPrefix")
    val allowDuplicates: Boolean = conf.getBoolean("dataset-comparison.allowDuplicates")

    DatasetComparisonConfig(errorColumnName, actualPrefix, expectedPrefix, allowDuplicates)
  }

  def default: DatasetComparisonConfig = fromTypeSafeConfig()
}
