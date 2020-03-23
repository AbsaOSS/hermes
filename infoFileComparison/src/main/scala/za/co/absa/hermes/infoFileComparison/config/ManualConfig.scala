package za.co.absa.hermes.infoFileComparison.config

class ManualConfig(
  val versionMetaKeys: List[String],
  val keysToIgnore: List[String]
) extends InfoFileComparisonConfig {}
