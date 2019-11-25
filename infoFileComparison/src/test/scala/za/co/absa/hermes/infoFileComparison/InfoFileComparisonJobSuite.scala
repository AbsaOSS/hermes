package za.co.absa.hermes.infoFileComparison

import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class InfoFileComparisonJobSuite extends FunSuite with BeforeAndAfterEach {
  val format = new SimpleDateFormat("yyyy_MM_dd-HH_mm_ss")
  var timePrefix = ""

  override def beforeEach(): Unit = {
    timePrefix = format.format(Calendar.getInstance().getTime)
  }

  test("Compare the same info files") {
    val outPath = s"file://target/test_output/comparison_job/positive/$timePrefix"

    val args = Array(
      "--new-path", s"file://${getClass.getResource("/info_file_original.json").getPath}",
      "--ref-path", s"file://${getClass.getResource("/info_file_correct.json").getPath}",
      "--out-path", outPath
    )
    InfoFileComparisonJob.main(args)

    assert(!Files.exists(Paths.get(outPath)))
  }

  test("Compare different info files") {
    val refPath = s"file://${getClass.getResource("/info_file_original.json").getPath}"
    val newPath = s"file://${getClass.getResource("/info_file_wrong.json").getPath}"
    val outPath = s"file://target/test_output/info_comparison/negative/$timePrefix"
    val message = s"Expected and actual info files differ.\nReference path: $refPath\n" +
                  s"Actual dataset path: $newPath\nDifference written to: $outPath"

    val args = Array(
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    val caught = intercept[InfoFilesDifferException] {
      InfoFileComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath.stripPrefix("file://"))))
  }
}
