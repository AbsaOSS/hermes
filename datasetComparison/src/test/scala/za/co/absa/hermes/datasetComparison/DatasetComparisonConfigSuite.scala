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

import org.scalatest.FunSuite

class DatasetComparisonConfigSuite extends FunSuite {
  test("Manual Config Correct") {
    val conf = DatasetComparisonConfig("errCol", "_actual", "_expected", false)
    assert(conf.validate().isSuccess)
    assert("errCol" == conf.errorColumnName)
    assert("_actual" == conf.actualPrefix)
    assert("_expected" == conf.expectedPrefix)
    assert(!conf.allowDuplicates)
  }

  test("Manual Config Bad Column Name") {
    val conf = DatasetComparisonConfig("errCol", "_actua l", "_expected", false)
    assert(conf.validate().isFailure)
  }

  test("Default Config Loaded") {
    val conf = DatasetComparisonConfig.default
    assert("errCol" == conf.errorColumnName)
    assert("actual" == conf.actualPrefix)
    assert("expected" == conf.expectedPrefix)
    assert(!conf.allowDuplicates)
  }

  test("Config with provided path loaded") {
    val conf = DatasetComparisonConfig.fromTypeSafeConfig(Some("confData/application.conf"))

    assert("errCol2" == conf.errorColumnName)
    assert("actual2" == conf.actualPrefix)
    assert("expected2" == conf.expectedPrefix)
    assert(conf.allowDuplicates)
  }
}
