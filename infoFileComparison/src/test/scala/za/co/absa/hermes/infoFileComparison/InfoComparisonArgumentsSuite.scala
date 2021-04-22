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

import scala.util.{Failure, Success}

class InfoComparisonArgumentsSuite extends FunSuite {
  private val newPath = "/tmp/standardized_out"
  private val refPath = "/tmp/reference_data"
  private val outPath = "/tmp/out_data"

//  Others are not tested as it is almost impossible to get a proper error
//  from scopt. This is hopefully going to be implemented in final stable 4.1
  test("Happy day scenario") {
    val cmdConfig = InfoComparisonArguments.getCmdLineArguments(
      Array(
        "--new-path", newPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    ) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(cmdConfig.newPath == newPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }

  test("Missing mandatory options") {
    val cmdConfig = InfoComparisonArguments.getCmdLineArguments(
      Array(
        "--new-path", newPath,
        "--out-path", outPath
      )
    ) match {
      case Success(_) => fail("InfoComparisonConfig returned while it should have thrown an error")
      case Failure(_) => succeed
    }
  }

  test("Ref and New path are the same") {
    val cmdConfig = InfoComparisonArguments.getCmdLineArguments(
      Array(
        "--new-path", newPath,
        "--ref-path", newPath,
        "--out-path", outPath
      )
    ) match {
      case Success(_) => fail("InfoComparisonConfig returned while it should have thrown an error")
      case Failure(_) => succeed
    }
  }
}
