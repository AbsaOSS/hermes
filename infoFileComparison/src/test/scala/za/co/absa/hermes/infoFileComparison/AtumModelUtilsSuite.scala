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

import java.io.File

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.utils.SerializationUtils
import za.co.absa.hermes.infoFileComparison.AtumModelUtils.ControlMeasureOps
import za.co.absa.hermes.utils.FileReader

class AtumModelUtilsSuite  extends FunSuite with BeforeAndAfterEach {
  private val fileOne = new File(getClass.getResource("/info_file_original.json").getFile).getAbsolutePath
  private val fileTwo = new File(getClass.getResource("/info_file_correct.json").getFile).getAbsolutePath
  private val fileThree = new File(getClass.getResource("/info_file_wrong.json").getFile).getAbsolutePath

  private val controlInfoJsonOne = FileReader.readFileAsString(fileOne)
  println(controlInfoJsonOne)
  private val controlMeasureOne: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](controlInfoJsonOne)

  private val controlInfoJsonTwo = FileReader.readFileAsString(fileTwo)
  private val controlMeasureTwo: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](controlInfoJsonTwo)

  private val controlInfoJsonThree = FileReader.readFileAsString(fileThree)
  private val controlMeasureThree: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](controlInfoJsonThree)

  private val config = InfoFileComparisonConfig.fromTypesafeConfig()

  test ("_INFO file no differences") {
    val diff: List[ModelDifference[_]] = controlMeasureOne.compareWith(controlMeasureTwo, config)
    assert(diff.isEmpty)
  }

  test ("_INFO file with differences") {
    val expectedDiff = List(
      ModelDifference("metadata.informationDate","01-01-2019","01-01-2020"),
      ModelDifference(
        "metadata.additionalInfo.std_cmd_line_args",
        "--menas-credentials-file /menas-credential.properties --dataset-name DeleteMe --dataset-version 115 --report-date 2019-01-01 --report-version 1 --raw-format json",
        "--menas-auth-keytab /creds.keytab --dataset-name DeleteMe --dataset-version 115 --report-date 2019-01-01 --report-version 1 --raw-format json")
    )
    val diff: List[ModelDifference[_]] = controlMeasureOne.compareWith(controlMeasureThree, config)
    println(expectedDiff == diff)
  }
}
