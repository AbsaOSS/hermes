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

package za.co.absa.hermes.datasetComparison.cliUtils

import za.co.absa.hermes.datasetComparison.MissingArgumentException

import scala.io.Source
import scala.util.{Failure, Success, Try}

case class CliOptions(referenceOptions: DataframeOptions,
                      newOptions: DataframeOptions,
                      outOptions: DataframeOptions,
                      keys: Set[String],
                      rawOptions: String,
                      schemaPath: Option[String] = None)

object CliOptions {
  def generateHelp(): Unit = {
    import CliHelpProtocol._
    import spray.json._

    val fileStream = this.getClass.getResourceAsStream("/cli_options.json")
    val jsonString = try { Source.fromInputStream(fileStream).mkString } finally fileStream.close()
    println(jsonString.parseJson.convertTo[CliHelp])
  }

  def parse(args: Array[String]): CliOptions = {
    if (args.contains("--help")) {
      generateHelp()
      System.exit(0)
    }

    val mapOfGroups: Map[String, String] = args.grouped(2).map{ a => (a(0).drop(2) -> a(1)) }.toMap
    val refMap = mapOfGroups.filterKeys(_ matches "ref-.*")
    val newMap = mapOfGroups.filterKeys(_ matches "new-.*")
    val schema = mapOfGroups.get("schema")
    val outMap = mapOfGroups.filterKeys(_ matches "out-.*")
    val keys = mapOfGroups.get("keys") match {
      case Some(x) => x.split(",").toSet
      case None    => Set.empty[String]
    }

    val genericMap = mapOfGroups -- refMap.keys -- newMap.keys -- outMap.keys -- Set("keys", "schema")

    val refMapWithoutPrefix = refMap.map { case (key, value) => (key.drop(4), value) }
    val newMapWithoutPrefix = newMap.map { case (key, value) => (key.drop(4), value) }
    val outMapWithoutPrefix = outMap.map { case (key, value) => (key.drop(4), value) }

    val finalRefMap = genericMap ++ refMapWithoutPrefix
    val finalNewMap = genericMap ++ newMapWithoutPrefix
    val finalOutMap = genericMap ++ outMapWithoutPrefix

    val refLoadOptions = Try(DataframeOptions.validateAndCreate(finalRefMap)) match {
      case Success(value)     => value
      case Failure(exception) =>
        val message = enrichMessage(exception.getMessage, "ref-")
        throw MissingArgumentException(message, exception)
    }

    val newLoadOptions = Try(DataframeOptions.validateAndCreate(finalNewMap)) match {
      case Success(value)     => value
      case Failure(exception) =>
        val message = enrichMessage(exception.getMessage, "new-")
        throw MissingArgumentException(message, exception)
    }

    val outDefaults = Map("format" -> "parquet")
    val outLoadOptions = Try(DataframeOptions.validateWithDefaultsAndCreate(finalOutMap, outDefaults)) match {
      case Success(value)     => value
      case Failure(exception) =>
        val message = enrichMessage(exception.getMessage, "out-")
        throw MissingArgumentException(message, exception)
    }

    CliOptions(refLoadOptions, newLoadOptions, outLoadOptions, keys, args.mkString(" "), schema)
  }

  /**
   * Adds a prefix to a key where there is an error. This then helps the message be more specific.
   * Example: If the issue is while parsing ref data. Message will say there is a missing "key"
   * and we want to say that it is either "key" or "ref-key", since it comes from ref.
   *
   * @param message The error message from parsing
   * @param keyPrefix Key prefix that will be added. Should be either "ref-" or "new-"
   * @return
   */
  private def enrichMessage(message: String, keyPrefix: String): String = {
    val exceptionMessagePattern = """(.*) ("--[a-z\-]+")""".r
    val exceptionMessagePattern(extractedMessage, key) = message
    val enrichedKey = key.patch(3, keyPrefix, 0)
    s"$extractedMessage $key or $enrichedKey"
  }
}
