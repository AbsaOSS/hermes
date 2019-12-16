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

import scala.io.Source

object FileReader {
  def readFileAsListOfLines(filename: String): List[String] =
    usingFile(Source.fromFile(filename)) { _.getLines().toList }

  def readFileAsString(filename: String, lineSeparator: String = "\n"): String =
    usingFile(Source.fromFile(filename)) { _.getLines().mkString(lineSeparator) }

  /**
    * Performs an operation using a resource, and then closes the resource, even if the operation throws an exception.
    * Copy of scala.util.Using form scala 2.13.0
    *
    * @param closeable the resource
    * @param f the function to perform with the resource
    * @tparam A the return type of the operation
    * @tparam B the type of the resource
    * @return the result of the operation, if neither the operation nor closing the resource throws
    */
  def usingFile[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
    try { f(closeable) } finally { closeable.close() }
}

