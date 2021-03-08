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

package za.co.absa.hermes.e2eRunner

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkBase {
  def sparkSession(name: String, sparkConf: Option[SparkConf] = None ): SparkSession = {
    val session = SparkSession.builder().appName(name)
    val withConf = if (sparkConf.isDefined) session.config(sparkConf.get) else session
    withConf.getOrCreate()
  }
}
