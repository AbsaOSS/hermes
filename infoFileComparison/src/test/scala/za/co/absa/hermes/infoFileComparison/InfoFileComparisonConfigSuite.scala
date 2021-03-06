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

import org.scalatest.FunSuite

class InfoFileComparisonConfigSuite extends FunSuite {
  test("Default TypeSafe Config Loaded") {
    val conf = InfoFileComparisonConfig.fromTypesafeConfig(None)
    val expectedKyesToIgnore = List("std_application_id", "conform_application_id", "conform_input_dir_size",
      "std_input_dir_size", "std_output_dir_size", "conform_output_dir_size")
    val expectedVersionKeys = List("std_enceladus_version", "conform_enceladus_version")
    assert(expectedKyesToIgnore == conf.keysToIgnore)
    assert(expectedVersionKeys == conf.versionMetaKeys)
  }

  test("TypeSafe Config with provided path loaded") {
    val conf = InfoFileComparisonConfig.fromTypesafeConfig(Some("confData/application.conf"))
    val expectedKyesToIgnore = List("customKeyToIgnore1", "customKeyToIgnore2")
    val expectedVersionKeys = List.empty[String]
    assert(expectedKyesToIgnore == conf.keysToIgnore)
    assert(expectedVersionKeys == conf.versionMetaKeys)
  }

  test("Empty Config") {
    val conf = InfoFileComparisonConfig.empty
    assert(conf.keysToIgnore.isEmpty)
    assert(conf.versionMetaKeys.isEmpty)
  }
}
