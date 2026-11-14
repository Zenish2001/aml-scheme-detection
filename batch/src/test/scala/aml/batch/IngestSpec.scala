package aml.batch

import java.nio.file.Files
import java.time.LocalDate
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IngestSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("IngestSpec")
    .master("local[2]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  // The real file's header, including its two columns both called "Account"
  private val header =
    "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering"

  // The first transaction of the FAN-OUT attempt in HI-Small_Patterns.txt
  private val good = "2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1"

  /** Writes a tiny CSV file with the real header and returns its path. */
  private def csvFile(rows: String*): String = {
    val file = Files.createTempFile("txns", ".csv")
    Files.writeString(file, (header +: rows).mkString("", "\n", "\n"))
    file.toString
  }

  behavior of "Ingest.readTransactions"

  it should "read a transaction with our own column names, keeping leading zeros" in {
    val row = Ingest.readTransactions(spark, csvFile(good)).collect().head
    row.getAs[String]("fromBank") shouldBe "021174"
    row.getAs[String]("toAcct") shouldBe "80011F990"
    row.getAs[Double]("amountPaid") shouldBe 2848.96
    row.getAs[Int]("isLaundering") shouldBe 1
    row.getAs[java.sql.Date]("day").toLocalDate shouldBe LocalDate.of(2022, 9, 1)
  }

  it should "keep bad rows, so they can be counted, and mark them invalid" in {
    val badAmount = "2022/09/01 00:07,021174,800737690,012,80011F990,abc,Euro,2848.96,Euro,ACH,0"
    val badTime   = "01-09-2022 00:08,021174,800737690,012,80011F990,10.00,Euro,10.00,Euro,ACH,0"
    val txns = Ingest.readTransactions(spark, csvFile(good, badAmount, badTime))
    txns.count() shouldBe 3
    txns.filter(Ingest.isValid).count() shouldBe 1
  }
}
