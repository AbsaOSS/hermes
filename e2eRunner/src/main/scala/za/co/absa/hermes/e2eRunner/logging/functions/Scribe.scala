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

package za.co.absa.hermes.e2eRunner.logging.functions

import scribe.Logger
import scribe.format._
import za.co.absa.hermes.e2eRunner.logging.LoggingFunctions

case class Scribe(logOrigin: String, formatter: Option[Formatter] = None) extends LoggingFunctions {
  private val defaultFormatter = formatter"$date $level ${string(logOrigin)} - $message$mdc"
  Logger.root.clearHandlers().withHandler(formatter = formatter.getOrElse(defaultFormatter)).replace()

  def debug(m: String, t: Throwable = None.orNull): Unit = scribe.debug(m, t)
  def info(m: String, t: Throwable = None.orNull): Unit = scribe.info(m, t)
  def warning(m: String, t: Throwable = None.orNull): Unit = scribe.warn(m, t)
  def error(m: String, t: Throwable = None.orNull): Unit = scribe.error(m, t)
}

object Scribe {
  def apply[T](klass: Class[T]): Scribe = { Scribe(klass.toString) }
  def apply[T](klass: Class[T], formatter: Option[Formatter]): Scribe = { Scribe(klass.toString, formatter) }
}
