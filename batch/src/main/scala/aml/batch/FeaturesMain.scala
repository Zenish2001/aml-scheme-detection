package aml.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Parts 2 and 2b: adds each transaction's features, checks them, compares laundering with normal transactions,
  * and saves the result.
  *
  *  - Part 2: what its sender and receiver did in the 24 hours before it (Features)
  *  - Part 2b: facts about the transaction, the sender's history with this receiver, how soon it pays out what
  *    it receives, and how this payment compares with its usual ones (TransactionFeatures)
  *
  * Run it from the sbt shell, after IngestMain:  batch/runMain aml.batch.FeaturesMain
  */
object FeaturesMain {

  val TxnsFolder     = "data/clean/txns"
  val FeaturesFolder = "data/clean/features"
  val SampleFraction = 0.0002   // about 1,000 transactions for the brute-force checks

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML features")

    try {
      // 1. Build the features. Each transaction gets an ID, so features can be joined back onto it.
      val txns     = spark.read.parquet(TxnsFolder).withColumn("txnId", monotonically_increasing_id()).cache()
      val txnCount = txns.count()

      val last24       = Features.last24Hours(Features.hourlyActivity(txns)).cache()
      val accountHours = last24.count()
      val accounts     = last24.select("account").distinct().count()

      val featured      = TransactionFeatures.addAll(Features.withAccountFeatures(txns, last24)).cache()
      val featuredCount = featured.count()
      val names         = (Features.featureColumns(featured) ++ TransactionFeatures.columns).distinct
      val nulls         = featured.filter(names.map(c => col(c).isNull).reduce(_ || _)).count()
      last24.unpersist()

      println()
      println(f"Transactions:                     $txnCount%,d")
      println(f"Accounts:                         $accounts%,d")
      println(f"Account-hours with any activity:  $accountHours%,d")
      println(f"Rows with features:               $featuredCount%,d   (expected $txnCount%,d)")
      println(f"Features per transaction:         ${names.size}%d")
      println(f"Rows with an empty feature:       $nulls%,d   (expected 0)")

      // 2. Check three features the slow, obvious way, on a sample of transactions
      val sample = featured
        .sample(withReplacement = false, fraction = SampleFraction, seed = 42)
        .withColumn("id", monotonically_increasing_id())
        .withColumn("t", unix_timestamp(col("ts")))
        .select("id", "senderAccount", "receiverAccount", "hour", "t",
          "senderSent24hCount", "pairPayments7d", "senderMinutesSinceReceived")
        .cache()
      println(f"\nBrute-force checks on ${sample.count()}%,d sampled transactions (mismatches, expected 0):")
      println(f"  senderSent24hCount:          ${sentMismatches(featured, sample)}%,d")
      println(f"  pairPayments7d:              ${pairMismatches(featured, sample)}%,d")
      println(f"  senderMinutesSinceReceived:  ${minutesMismatches(featured, sample)}%,d")

      // 3. How laundering transactions differ: medians, and how often each feature is above zero
      compare(featured, names)

      // 4. Save
      featured.write.mode("overwrite").partitionBy("day").parquet(FeaturesFolder)
      println(f"\nSaved $featuredCount%,d transactions with ${names.size}%d features to $FeaturesFolder")
    } finally spark.stop()
  }

  /** The sender's payments in the 24 hours before the transaction's hour, counted one by one. */
  private def sentMismatches(featured: DataFrame, sample: DataFrame): Long = {
    val payments = featured.select(col("senderAccount"), col("hour").as("paymentHour"))
    val counted = sample.join(payments, Seq("senderAccount"))
      .filter(col("paymentHour").between(col("hour") - Features.WindowHours, col("hour") - 1))
      .groupBy("id").count()
    sample.join(counted, Seq("id"), "left")
      .filter(coalesce(col("count"), lit(0L)) =!= col("senderSent24hCount")).count()
  }

  /** The sender's payments to the same receiver in the 168 hours before, counted one by one. */
  private def pairMismatches(featured: DataFrame, sample: DataFrame): Long = {
    val payments = featured.select(col("senderAccount"), col("receiverAccount"), col("hour").as("paymentHour"))
    val counted = sample.join(payments, Seq("senderAccount", "receiverAccount"))
      .filter(col("paymentHour").between(col("hour") - TransactionFeatures.PairWindowHours, col("hour") - 1))
      .groupBy("id").count()
    sample.join(counted, Seq("id"), "left")
      .filter(coalesce(col("count"), lit(0L)) =!= col("pairPayments7d")).count()
  }

  /** The sender's latest receipt before the transaction's minute, found by looking at all of its receipts. */
  private def minutesMismatches(featured: DataFrame, sample: DataFrame): Long = {
    val receipts = featured.select(col("receiverAccount").as("senderAccount"), unix_timestamp(col("ts")).as("receivedT"))
    val latest = sample.join(receipts, Seq("senderAccount"))
      .filter(col("receivedT") < col("t"))
      .groupBy("id").agg(max("receivedT").as("lastReceived"))
    val expected = least(
      coalesce(((col("t") - col("lastReceived")) / 60).cast("long"), lit(TransactionFeatures.MaxMinutes)),
      lit(TransactionFeatures.MaxMinutes))
    sample.join(latest, Seq("id"), "left")
      .filter(expected =!= col("senderMinutesSinceReceived")).count()
  }

  /** Prints each feature's median and share above zero, for normal and laundering transactions. */
  private def compare(featured: DataFrame, names: Seq[String]): Unit = {
    val stats = names.flatMap { c =>
      Seq(percentile_approx(col(c), lit(0.5), lit(1000)).cast("double").as(s"${c}_median"),
          avg(when(col(c) > 0, 1.0).otherwise(0.0)).as(s"${c}_aboveZero"))
    }
    val byFlag = featured.groupBy("isLaundering").agg(stats.head, stats.tail: _*).collect()
      .map(row => row.getAs[Int]("isLaundering") -> row).toMap

    println("\nNormal against laundering transactions: medians, and the share of transactions above zero")
    println("%-28s %14s %14s %10s %10s".format("feature", "median normal", "median laund.", "> 0 normal", "> 0 laund."))
    names.foreach { c =>
      def median(flag: Int)    = byFlag(flag).getAs[Double](s"${c}_median")
      def aboveZero(flag: Int) = byFlag(flag).getAs[Double](s"${c}_aboveZero") * 100
      println("%-28s %,14.2f %,14.2f %9.1f%% %9.1f%%".format(c, median(0), median(1), aboveZero(0), aboveZero(1)))
    }
  }
}
