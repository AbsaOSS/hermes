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

package za.co.absa.hermes.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkTestBase { self =>
  val config: Config = ConfigFactory.load()
  val sparkMaster: String = config.getString("utils.sparkTestBaseMaster")

  System.setProperty("user.timezone", "UTC")

  // Do not display INFO entries for tests
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  implicit val spark: SparkSession = SparkSession.builder()
    .master(sparkMaster)
    .appName(s"Hermes unit test - ${self.getClass.getName}")
    .config("spark.ui.enabled", "false")
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.hive.convertMetastoreParquet", false)
    .config("spark.sql.codegen.wholeStage", value = false)
    .config("fs.defaultFS","file:/")
    .getOrCreate()
}
