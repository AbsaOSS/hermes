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

import org.scalatest.FunSuite

import scala.util.{Failure, Success}

class E2ERunnerConfigTest extends FunSuite {
  private val randomPath = "/alfa/beta"

  test("GetCmdLineArguments - positive minimal") {
    val cmd = E2ERunnerConfig.getCmdLineArguments(Array(
      "--test-definition-path", randomPath
    )) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(randomPath == cmd.testDefinition)
    assert(cmd.jarPath.isEmpty)
    assert(!cmd.failFast)
  }

  test("GetCmdLineArguments - missing test definition") {
    val actualException = E2ERunnerConfig.getCmdLineArguments(Array()) match {
      case Success(_) => fail("Command line parsing passed but shouldn't")
      case Failure(exception) => exception
    }

    assert("Wrong options provided. List can be found above"  == actualException.getMessage)
  }

  test("GetCmdLineArguments - positive full") {
    val cmd = E2ERunnerConfig.getCmdLineArguments(Array(
      "--test-definition-path", randomPath,
      "--jar-path", "/some/path",
      "--fail-fast", "false",
      "--extra-vars", """some=value,some2=value with spaces"""
    )) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(randomPath == cmd.testDefinition)
    assert(cmd.jarPath.isDefined)
    assert("/some/path" == cmd.jarPath.get)
    assert(!cmd.failFast)
    assert(Map("some" -> "value", "some2" -> "value with spaces") == cmd.extraVars)
  }

}
