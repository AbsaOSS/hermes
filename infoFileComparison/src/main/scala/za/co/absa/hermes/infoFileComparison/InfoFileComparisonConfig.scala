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

package za.co.absa.hermes.infoFileComparison

import com.typesafe.config.{Config, ConfigFactory}

case class InfoFileComparisonConfig(versionMetaKeys: List[String], keysToIgnore: List[String]) {
  def getLoggableString: String = {
    s"""Effective InfoFileComparison configuration:
       | Meta Keys (indicating versions) only to be printed:
       |  ${versionMetaKeys.mkString("\n  ")}
       | Meta Keys to be ignored:
       |  ${keysToIgnore.mkString("\n  ")}""".stripMargin
  }
}

object InfoFileComparisonConfig {
  def fromTypesafeConfig(path: Option[String] = None): InfoFileComparisonConfig = {
    val conf: Config = path match {
      case Some(x) => ConfigFactory.load(x)
      case None    => ConfigFactory.load()
    }

    import collection.JavaConversions._

    val versionMetaKeys = conf.getStringList("info-file-comparison.atum-models.versionMetaKeys").toList
    val keysToIgnore = conf.getStringList("info-file-comparison.atum-models.ignoredMetaKeys").toList
    InfoFileComparisonConfig(versionMetaKeys, keysToIgnore)
  }

  def empty: InfoFileComparisonConfig = InfoFileComparisonConfig(List.empty[String], List.empty[String])
}
