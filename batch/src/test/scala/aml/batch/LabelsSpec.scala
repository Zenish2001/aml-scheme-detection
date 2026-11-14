package aml.batch

import java.nio.file.Files
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LabelsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("LabelsSpec")

  override def afterAll(): Unit = spark.stop()

  private val header =
    "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering"

  // Three transactions: laundering and listed in an attempt, laundering but not listed, and normal
  private val listed   = "2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1"
  private val unlisted = "2022/09/01 00:04,0119,811C597B0,0048309,811C599A0,34254.65,Saudi Riyal,34254.65,Saudi Riyal,ACH,1"
  private val normal   = "2022/09/02 13:16,001,800149630,001,80021A080,5194.01,US Dollar,5194.01,US Dollar,ACH,0"

  private def csvFile(rows: String*): String = {
    val file = Files.createTempFile("txns", ".csv")
    Files.writeString(file, (header +: rows).mkString("", "\n", "\n"))
    file.toString
  }

  behavior of "Labels.withSchemes"

  it should "give each laundering transaction its scheme or UNASSIGNED, and normal ones none" in {
    val txns     = Ingest.readTransactions(spark, csvFile(listed, unlisted, normal))
    val attempts = List(Attempt("FAN-OUT", List(listed)))
    val labelled = Labels.withSchemes(txns, Labels.attemptTransactions(spark, attempts))

    val schemeByAccount = labelled.select("fromAcct", "scheme").collect()
      .map(row => row.getString(0) -> Option(row.getString(1))).toMap

    labelled.count() shouldBe 3   // labelling adds and drops no rows
    schemeByAccount("800737690") shouldBe Some("FAN-OUT")
    schemeByAccount("811C597B0") shouldBe Some(Labels.Unassigned)
    schemeByAccount("800149630") shouldBe None
  }
}
