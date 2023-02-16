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

package za.co.absa.hermes.utils

import za.co.absa.commons.version.Version._
import za.co.absa.commons.version.impl.SemVer20Impl.SemanticVersion

object SparkCompatibility {
  val minSparkVersionIncluded: SemanticVersion = semver"2.4.2"
  val maxSparkVersionExcluded: SemanticVersion = semver"3.3.0"

  def checkVersion(sparkVersion: String): Unit = {
    val version = asSemVer(sparkVersion)
    require(version >= minSparkVersionIncluded && version < maxSparkVersionExcluded,
      s"Spark version must be more than $minSparkVersionIncluded (including) and less than (excluding) $maxSparkVersionExcluded")
  }
}
