package za.co.absa.hermes.datasetComparison

import net.liftweb.json.DefaultFormats
import org.apache.commons.cli.MissingArgumentException

import scala.io.Source

case class CliOptions(referenceOptions: DataframeOptions,
                      newOptions: DataframeOptions,
                      outPath: String,
                      keys: Option[Set[String]])

object CliOptions {
  def generateHelp: Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val fileStream = getClass.getResourceAsStream("/cli_options.json")
    val jsonString = Source.fromInputStream(fileStream).mkString
    val json = net.liftweb.json.parse(jsonString)
    println(json.extract[CliHelp])
  }

  def parse(args: Array[String]): CliOptions = {
    if (args.indexOf("--help") >= 0) {
      generateHelp
      System.exit(0)
    }

    val mapOfGroups: Map[String, String] = args.grouped(2).map{ a => (a(0).drop(2) -> a(1)) }.toMap
    val refMap = mapOfGroups.filterKeys(_ matches "ref-.*")
    val newMap = mapOfGroups.filterKeys(_ matches "new-.*")
    val keys = mapOfGroups.get("keys").map { x => x.split(",").toSet }
    val outPath = mapOfGroups.getOrElse("outPath", throw new MissingArgumentException("""outPath is mandatory option. Use "--outPath"."""))
    val genericMap = mapOfGroups -- refMap.keys -- newMap.keys -- Set("keys", "outPath")

    val refMapWithoutPrefix = refMap.map { case (key, value) => (key.drop(4), value) }
    val newMapWithoutPrefix = newMap.map { case (key, value) => (key.drop(4), value) }

    val finalRefMap = genericMap ++ refMapWithoutPrefix
    val finalNewMap = genericMap ++ newMapWithoutPrefix

    val refLoadOptions = DataframeOptions.validateAndCreate(finalRefMap)
    val newLoadOptions = DataframeOptions.validateAndCreate(finalNewMap)

    CliOptions(refLoadOptions, newLoadOptions, outPath, keys)
  }
}
