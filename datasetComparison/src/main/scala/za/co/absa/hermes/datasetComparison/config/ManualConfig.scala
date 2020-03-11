package za.co.absa.hermes.datasetComparison.config

class ManualConfig(
  val errorColumnName: String,
  val tmpColumnName: String,
  val comparisonUniqueId: String,
  val actualPrefix: String,
  val expectedPrefix: String,
  val allowDuplicates: Boolean
) extends DatasetComparisonConfig {}
