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
import za.co.absa.hermes.datasetComparison.dataFrame.Parameters

import scala.io.Source
import scala.util.{Failure, Success, Try}

private case class OptionsTrio(reference: Map[String, String],
                               actual: Map[String, String],
                               output: Map[String, String]) {
  private[cliUtils] def mergeWithProvidedDefaults(genericMap: Map[String, String]): OptionsTrio = {
    OptionsTrio(
      genericMap ++ reference,
      genericMap ++ actual,
      genericMap ++ output
    )
  }

  private[cliUtils] def dropOptionPrefix: OptionsTrio = {
    OptionsTrio(
      CliParametersParser.dropOptionPrefix(reference),
      CliParametersParser.dropOptionPrefix(actual),
      CliParametersParser.dropOptionPrefix(output)
    )
  }
}

object CliParametersParser {
  private val argumentPrefix: String = "--"
  private val argumentPrefixSize: Int = argumentPrefix.length
  private val actualPrefix = "new-"
  private val referencePrefix = "ref-"
  private val outputPrefix = "out-"
  private val outputDefaults = Map("format" -> "parquet")

  def generateHelp(): Unit = {
    import CliHelpJsonProtocol._
    import spray.json._

    val fileStream = this.getClass.getResourceAsStream("/cli_options.json")
    val jsonString = try { Source.fromInputStream(fileStream).mkString } finally fileStream.close()
    println(jsonString.parseJson.convertTo[CliHelp])
  }

  def parse(args: Array[String]): CliParameters = {
    require(args.nonEmpty, "No arguments for reader and wirter passed")
    require(args.length == 1 || args.length % 2 == 0, "Number of arguments must be either one, for help, or even")

    if (args.length == 1) {
      generateHelp()
      if (args.head == "--help") System.exit(0)
      else throw new IllegalArgumentException("Single unknown argument provided. Printed help")
    }

    val (schema, keys, trioWithDefaultsMerged) = genericTrioParse(args)
    val finalOutMapWithDefaults = trioWithDefaultsMerged.copy(output = outputDefaults ++ trioWithDefaultsMerged.output)

    val refLoadOptions = getLoadOptions(finalOutMapWithDefaults.reference, referencePrefix)
    val newLoadOptions = getLoadOptions(finalOutMapWithDefaults.actual, actualPrefix)
    val outLoadOptions = getLoadOptions(finalOutMapWithDefaults.output, outputPrefix)

    CliParameters(refLoadOptions, newLoadOptions, Some(outLoadOptions), keys, args.mkString(" "), schema)
  }

  def parseInputParameters(args: Array[String]): CliParameters = {
    require(args.nonEmpty, "No arguments for reader passed")
    require(args.length % 2 == 0, "Number of arguments must be even")

    val (schema, keys, finalOutMapWithDefaults) = genericTrioParse(args)

    val refLoadOptions = getLoadOptions(finalOutMapWithDefaults.reference, referencePrefix)
    val newLoadOptions = getLoadOptions(finalOutMapWithDefaults.actual, actualPrefix)

    CliParameters(refLoadOptions, newLoadOptions, None, keys, args.mkString(" "), schema)
  }

  def parseOutputParameters(args: Array[String]): Parameters = {
    require(args.nonEmpty, "No arguments for writer passed")
    require(args.length % 2 == 0, "Number of arguments must be even")

    val mapOfAllOptions = arrayToMap(args)
    val (OptionsTrio(_, _, outMap), genericMap) = getMapOfOptions(mapOfAllOptions)
    val outMapWithoutPrefix = dropOptionPrefix(outMap)
    val finalOutMap = genericMap ++ outMapWithoutPrefix
    val finalOutMapWithDefaults = outputDefaults ++ finalOutMap

    getLoadOptions(finalOutMapWithDefaults, outputPrefix)
  }

  private[cliUtils] def genericTrioParse(args: Array[String]): (Option[String], Set[String], OptionsTrio) = {
    val mapOfAllOptions = arrayToMap(args)

    val schema = mapOfAllOptions.get("schema")
    val keys = mapOfAllOptions.get("keys") match {
      case Some(x) => x.split(",").toSet
      case None => Set.empty[String]
    }

    val (rawTrio, genericMap) = getMapOfOptions(mapOfAllOptions)

    val trioWithoutPrefix = rawTrio.dropOptionPrefix
    val trioWithDefaultsMerged = trioWithoutPrefix.mergeWithProvidedDefaults(genericMap)
    (schema, keys, trioWithDefaultsMerged)
  }

  private[cliUtils] def dropOptionPrefix(refMap: Map[String, String]): Map[String, String] = {
    refMap.map { case (key, value) => (key.drop(4), value) }
  }

  private[cliUtils] def getMapOfOptions(opts: Map[String, String]): (OptionsTrio, Map[String, String]) = {
    val refMap = opts.filterKeys(_ matches s"$referencePrefix.*")
    val newMap = opts.filterKeys(_ matches s"$actualPrefix.*")
    val outMap = opts.filterKeys(_ matches s"$outputPrefix.*")
    val genericMap = opts -- refMap.keys -- newMap.keys -- outMap.keys -- Set("keys", "schema")
    (OptionsTrio(refMap, newMap, outMap), genericMap)
  }

  private[cliUtils] def arrayToMap(args: Array[String]): Map[String, String] = {
    args.grouped(2).map { case Array(option, value) => option.drop(argumentPrefixSize) -> value }.toMap
  }

  /**
   * Validate and create DataframeOption from the map of options provided
   *
   * @param mapOfOptions Map of options where key is option and value is option value
   * @param keyPrefix Prefix to be added in case options are bad and need an error message. This help with
   *                  distinguishing from which option type (ref, new, out) the issue came.
   * @return Returns the DataframeOptions
   */
  private[cliUtils] def getLoadOptions(mapOfOptions: Map[String, String], keyPrefix: String): Parameters = {
    Try(Parameters.validateAndCreate(mapOfOptions)) match {
      case Success(value) => value
      case Failure(exception) =>
        val message = enrichMessage(exception.getMessage, keyPrefix)
        throw MissingArgumentException(message, exception)
    }
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
  private[cliUtils] def enrichMessage(message: String, keyPrefix: String): String = {
    val exceptionMessagePattern = """(.*) ("--[a-z\-]+")""".r
    val exceptionMessagePattern(extractedMessage, key) = message
    val enrichedKey = key.patch(3, keyPrefix, 0)
    s"$extractedMessage $key or $enrichedKey"
  }
}

