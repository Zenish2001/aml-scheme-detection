package aml.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FeaturesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("FeaturesSpec")

  override def afterAll(): Unit = spark.stop()

  /** Tiny transactions: (time, from account, to account, US dollars), all at bank 001, in dollars, by ACH. */
  private def txns(rows: (String, String, String, Double)*): DataFrame = {
    val session = spark
    import session.implicits._
    rows.toDF("time", "fromAcct", "toAcct", "amountPaidUsd")
      .withColumn("ts", to_timestamp(col("time"), "yyyy-MM-dd HH:mm"))
      .withColumn("fromBank", lit("001"))
      .withColumn("toBank", lit("001"))
      .withColumn("amountReceivedUsd", col("amountPaidUsd"))
      .withColumn("payCcy", lit("US Dollar"))
      .withColumn("recvCcy", lit("US Dollar"))
      .withColumn("format", lit("ACH"))
  }

  private def featured(input: DataFrame): DataFrame =
    Features.withAccountFeatures(input, Features.last24Hours(Features.hourlyActivity(input)))

  /** One feature, looked up by each transaction's time. */
  private def byTime(df: DataFrame, feature: String): Map[String, Long] =
    df.select(col("time"), col(feature).cast("long")).collect().map(row => row.getString(0) -> row.getLong(1)).toMap

  behavior of "Features"

  it should "count the sender's payments in the 24 whole hours before the transaction's hour" in {
    val result = featured(txns(
      ("2022-09-01 10:00", "A", "X", 10.0),
      ("2022-09-01 10:30", "A", "X", 10.0),   // same hour as the first: not counted for it
      ("2022-09-01 15:00", "A", "Y", 10.0),
      ("2022-09-02 10:15", "A", "X", 10.0),   // exactly 24 hours after 10:00 sees 10:00, 10:30 and 15:00
      ("2022-09-02 11:00", "A", "Z", 10.0)))  // by now, 10:00 and 10:30 have dropped out

    byTime(result, "senderSent24hCount") shouldBe Map(
      "2022-09-01 10:00" -> 0L, "2022-09-01 10:30" -> 0L, "2022-09-01 15:00" -> 2L,
      "2022-09-02 10:15" -> 3L, "2022-09-02 11:00" -> 2L)
    byTime(result, "senderSent24hParties")("2022-09-02 10:15") shouldBe 2L   // X and Y
  }

  it should "show what the sender received just before paying it on" in {
    val result = featured(txns(
      ("2022-09-01 10:00", "A", "B", 100.0),
      ("2022-09-01 10:20", "A", "B", 50.0),
      ("2022-09-01 10:40", "C", "B", 30.0),
      ("2022-09-01 12:00", "B", "D", 170.0)))   // B passes the money on
    val passOn = result.filter(col("time") === "2022-09-01 12:00").collect().head

    passOn.getAs[Long]("senderRecv24hCount") shouldBe 3L
    passOn.getAs[Double]("senderRecv24hUsd") shouldBe 180.0
    passOn.getAs[Int]("senderRecv24hParties") shouldBe 2   // A and C
    passOn.getAs[Long]("senderSent24hCount") shouldBe 0L
    passOn.getAs[Long]("receiverRecv24hCount") shouldBe 0L
  }

  it should "keep every transaction, with no empty features" in {
    val input  = txns(("2022-09-01 10:00", "A", "B", 1.0), ("2022-09-01 11:00", "B", "C", 1.0), ("2022-09-03 09:00", "C", "A", 1.0))
    val result = featured(input)
    val names  = Features.featureColumns(result)

    result.count() shouldBe 3
    names should have length 20
    result.filter(names.map(c => col(c).isNull).reduce(_ || _)).count() shouldBe 0
  }
}
