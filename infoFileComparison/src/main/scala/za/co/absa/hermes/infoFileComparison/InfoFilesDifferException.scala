package za.co.absa.hermes.infoFileComparison


final case class InfoFilesDifferException(refPath: String,
                                          stdPath: String,
                                          outPath: String)
  extends Exception("Expected and actual info files differ.\n" +
    s"Reference path: $refPath\n" +
    s"Actual dataset path: $stdPath\n" +
    s"Difference written to: $outPath")
