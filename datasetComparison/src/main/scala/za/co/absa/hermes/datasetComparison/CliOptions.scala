package za.co.absa.hermes.datasetComparison

import org.apache.commons.cli.MissingArgumentException

case class CliOptions(referenceOptions: DataframeOptions,
                      newOptions: DataframeOptions,
                      outPath: String,
                      keys: Option[Set[String]])

object CliOptions {
//  def generateHelp: Unit = ???

  def parse(args: Array[String]): CliOptions = {
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
