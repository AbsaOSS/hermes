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

package za.co.absa.hermes.e2eRunner

import scopt.OptionParser

import scala.util.{Failure, Success, Try}

/**
  * This is a class for configuration provided by the command line parameters
  *
  * Note: scopt requires all fields to have default values.
  *       Even if a field is mandatory it needs a default value.
  */
case class E2ERunnerConfigExperimental(testDefinition: String = "",
                                       jarPath: Option[String] = None,
                                       failfast: Boolean = true)

object E2ERunnerConfigExperimental {
  /**
   * Parses and validates an Array of input parameters and creates an instance of CmdConfig case class
   *
   * @param args Array of argument to be parsed
   * @return Returns a CmdConfig instance holding parameters passed
   */
  def getCmdLineArguments(args: Array[String]): Try[E2ERunnerConfigExperimental] = {
    val parser = new CmdParser("spark-submit [spark options] TestUtils.jar")

    parser.parse(args, E2ERunnerConfigExperimental()) match {
      case Some(config) => Success(config)
      case _            => Failure(new IllegalArgumentException("Wrong options provided. List can be found above"))
    }
  }

  private class CmdParser(programName: String) extends OptionParser[E2ERunnerConfigExperimental](programName) {
    head("\nEnd2End Test Runner")

    opt[String]("test-definition-path")
      .required
      .action((value, config) => { config.copy(testDefinition = value) })
      .text("Path to a json with test definitions")

    opt[String]("jar-path")
      .optional
      .action((value, config) => {
        if (value.isEmpty) {
          config.copy(jarPath = None)
        } else {
          config.copy(jarPath = Some(value))
        }
      })
      .text("Path to a JAR files with plugin definitions")

    opt[Boolean]("fail-fast")
      .optional
      .action((value, config) => { config.copy(failfast = value) })
      .text("Should tests exit if one of them has a non-zero exit")

    help("help").text("prints this usage text")
  }
}


