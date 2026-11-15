package aml.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import TransactionFeatures.{MaxMinutes, NoHistory}

class TransactionFeaturesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("TransactionFeaturesSpec")

  override def afterAll(): Unit = spark.stop()

  /** Tiny transactions: (time, sender, receiver, US dollars), with the columns the features need. */
  private def txns(rows: (String, String, String, Double)*): DataFrame = {
    val session = spark
    import session.implicits._
    rows.toDF("time", "senderAccount", "receiverAccount", "amountPaidUsd")
      .withColumn("ts", to_timestamp(col("time"), "yyyy-MM-dd HH:mm"))
      .withColumn("hour", Features.hourOf(col("ts")))
      .withColumn("txnId", monotonically_increasing_id())
  }

  /** One feature, looked up by each transaction's time and sender. */
  private def byTimeAndSender(df: DataFrame, feature: String): Map[(String, String), Long] =
    df.select(col("time"), col("senderAccount"), col(feature).cast("long")).collect()
      .map(row => (row.getString(0), row.getString(1)) -> row.getLong(2)).toMap

  behavior of "TransactionFeatures"

  it should "flag facts about the transaction itself" in {
    val session = spark
    import session.implicits._
    val facts = TransactionFeatures.withTransactionFacts(
      Seq(("001|A", "001|A", "001", "001", "Euro", "US Dollar", 9500.0, "2022-09-01 14:30"),
          ("001|A", "002|B", "001", "002", "Euro", "Euro", 120.0, "2022-09-01 03:05"))
        .toDF("senderAccount", "receiverAccount", "fromBank", "toBank", "payCcy", "recvCcy", "amountPaidUsd", "time")
        .withColumn("ts", to_timestamp(col("time"), "yyyy-MM-dd HH:mm")))
      .orderBy("time").collect()

    val Seq(night, afternoon) = facts.toSeq
    night.getAs[Int]("isCrossCurrency") shouldBe 0
    night.getAs[Int]("isSelfTransfer") shouldBe 0
    night.getAs[Int]("isSameBank") shouldBe 0
    night.getAs[Int]("hourOfDay") shouldBe 3
    night.getAs[Int]("isJustUnder10k") shouldBe 0
    afternoon.getAs[Int]("isCrossCurrency") shouldBe 1
    afternoon.getAs[Int]("isSelfTransfer") shouldBe 1
    afternoon.getAs[Int]("isSameBank") shouldBe 1
    afternoon.getAs[Int]("hourOfDay") shouldBe 14
    afternoon.getAs[Int]("isJustUnder10k") shouldBe 1
  }

  it should "count earlier payments to the same receiver in the 168 whole hours before" in {
    val result = TransactionFeatures.withPairHistory(txns(
      ("2022-09-01 10:00", "A", "B", 1.0),
      ("2022-09-01 12:00", "A", "B", 1.0),
      ("2022-09-01 13:00", "A", "C", 1.0),    // a new payee
      ("2022-09-08 11:00", "A", "B", 1.0)))   // 169 hours after the first: only 12:00 is still within the week

    byTimeAndSender(result, "pairPayments7d") shouldBe Map(
      ("2022-09-01 10:00", "A") -> 0L, ("2022-09-01 12:00", "A") -> 1L,
      ("2022-09-01 13:00", "A") -> 0L, ("2022-09-08 11:00", "A") -> 1L)
  }

  it should "measure the minutes since the sender last received money, ignoring the same minute" in {
    val result = TransactionFeatures.withMinutesSinceReceived(txns(
      ("2022-09-01 10:00", "X", "A", 100.0),
      ("2022-09-01 10:25", "A", "Y", 100.0),   // 25 minutes after A received
      ("2022-09-01 11:00", "B", "A", 50.0),
      ("2022-09-01 11:00", "A", "W", 50.0),    // the 11:00 receipt is the same minute, so 10:00 counts
      ("2022-09-01 12:00", "P", "R", 10.0)))   // P never received anything

    byTimeAndSender(result, "senderMinutesSinceReceived") shouldBe Map(
      ("2022-09-01 10:00", "X") -> MaxMinutes, ("2022-09-01 10:25", "A") -> 25L,
      ("2022-09-01 11:00", "B") -> MaxMinutes, ("2022-09-01 11:00", "A") -> 60L,
      ("2022-09-01 12:00", "P") -> MaxMinutes)
  }

  it should "compare the payment with the sender's last 24 hours, or give -1 with nothing to compare" in {
    val session = spark
    import session.implicits._
    // received 100 and sent 2 payments totalling 50 in the last 24 hours, then pays 50; and an account with no history
    val ratios = TransactionFeatures.withRatios(
      Seq(("A", 100.0, 50.0, 2L, 50.0), ("B", 0.0, 0.0, 0L, 30.0))
        .toDF("senderAccount", "senderRecv24hUsd", "senderSent24hUsd", "senderSent24hCount", "amountPaidUsd"))
      .orderBy("senderAccount").collect()

    ratios(0).getAs[Double]("senderPassThrough24h") shouldBe 1.0       // (50 + 50) / 100
    ratios(0).getAs[Double]("amountVsSenderAverage24h") shouldBe 2.0   // 50 / (50 / 2)
    ratios(1).getAs[Double]("senderPassThrough24h") shouldBe NoHistory
    ratios(1).getAs[Double]("amountVsSenderAverage24h") shouldBe NoHistory
  }
}
