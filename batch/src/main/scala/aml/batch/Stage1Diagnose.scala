package aml.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Part 4a.2: why are the Stage 1 model's highest scores mostly false alarms? Looks at the saved test-day scores:
  * precision by rank, who the false alarms are, how they differ from the laundering the model does catch, and
  * what happens if alerts are raised per sending account instead of per transaction.
  *
  * Run it from the sbt shell, after Stage1Main:  batch/runMain aml.batch.Stage1Diagnose
  */
object Stage1Diagnose {

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML stage 1 diagnosis")

    try {
      val features = spark.read.parquet(Stage1Main.GraphFolder)
      val hubs     = GraphFeatures.hubs(features).cache()
      val scores   = spark.read.parquet(Stage1Main.ScoresFolder).filter(col("period") === "test").select("txnId", "score")
      val scored   = scores.join(features, Seq("txnId"))
        .join(hubs.withColumn("senderIsHub", lit(1)).withColumnRenamed("account", "senderAccount"), Seq("senderAccount"), "left")
        .join(hubs.withColumn("receiverIsHub", lit(1)).withColumnRenamed("account", "receiverAccount"), Seq("receiverAccount"), "left")
        .withColumn("touchesHub", (coalesce(col("senderIsHub"), lit(0)) + coalesce(col("receiverIsHub"), lit(0)) > 0).cast("int"))
        .withColumn("rank", row_number().over(Window.partitionBy("day").orderBy(desc("score"))))
        .cache()

      // 1. Precision by rank: are the very top alerts really the worst?
      println("\nPrecision by daily rank, on the test days:")
      scored.filter(col("rank") <= 250)
        .withColumn("band", when(col("rank") <= 25, "1. ranks 1-25").when(col("rank") <= 50, "2. ranks 26-50")
          .when(col("rank") <= 100, "3. ranks 51-100").otherwise("4. ranks 101-250"))
        .groupBy("band").agg(count(lit(1)).as("alerts"), round(avg(col("isLaundering")) * 100, 1).as("precision %"))
        .orderBy("band").show(truncate = false)

      // 2. Who the top false alarms are
      val top100       = scored.filter(col("rank") <= 25)
      val falseTop     = top100.filter(col("isLaundering") === 0)
      val topCount     = top100.count()
      val falseCount   = falseTop.count()
      val falseSenders = falseTop.select("senderAccount").distinct().count()
      println(f"The top 25 alerts of each day: $topCount%,d alerts, $falseCount%,d false alarms, from $falseSenders%,d different senders")
      println("Senders with the most false alarms in the top 25s:")
      falseTop.groupBy("senderAccount").agg(count(lit(1)).as("falseAlarms"), max("touchesHub").as("hub"))
        .orderBy(desc("falseAlarms")).show(5, truncate = false)
      println("Distinct scores among the top 100 alerts (few would mean ties decide the order):")
      top100.agg(countDistinct("score").as("distinctScores"), round(max("score"), 4).as("highest"), round(min("score"), 4).as("lowest")).show()

      // 3. False alarms against caught laundering, in the top 250 of each day
      val top1000 = scored.filter(col("rank") <= 250).withColumn("kind", when(col("isLaundering") === 1, "caught").otherwise("false alarm"))
      println("In the top 250 of each day, caught laundering against false alarms:")
      top1000.groupBy("kind").agg(
          count(lit(1)).as("alerts"),
          countDistinct("senderAccount").as("senders"),
          round(percentile_approx(col("amountPaidUsd"), lit(0.5), lit(1000)), 0).as("medianUsd"),
          round(avg("touchesHub") * 100, 1).as("hub %"),
          round(avg("isSelfTransfer") * 100, 1).as("self %"),
          percentile_approx(col("pairPayments7d"), lit(0.5), lit(1000)).as("medPair7d"),
          percentile_approx(col("senderMinutesSinceReceived"), lit(0.5), lit(1000)).as("medMinutes"),
          percentile_approx(col("senderSent24hParties"), lit(0.5), lit(1000)).as("medParties"))
        .orderBy("kind").show(truncate = false)
      println("Their payment formats:")
      top1000.groupBy("format").pivot("kind", Seq("caught", "false alarm")).count().na.fill(0).orderBy(desc("false alarm")).show(truncate = false)

      // 4. One alert per sending account per day, instead of one per transaction
      val bySender = scored.groupBy("day", "senderAccount")
        .agg(max("score").as("score"), max("isLaundering").as("anyLaundering"), sum("isLaundering").as("launderingTxns"))
        .withColumn("rank", row_number().over(Window.partitionBy("day").orderBy(desc("score"))))
        .cache()
      val totalLaundering = scored.filter(col("isLaundering") === 1).count()
      println("Alerts per transaction against alerts per sending account (an account alert covers its whole day):")
      println("%-16s %22s %22s %22s".format("alerts per day", "transaction precision", "account precision", "laundering covered"))
      Seq(25, 50, 100, 250).foreach { perDay =>
        val txnPrecision = scored.filter(col("rank") <= perDay).agg(avg("isLaundering")).first().getDouble(0)
        val top          = bySender.filter(col("rank") <= perDay)
        val accPrecision = top.agg(avg("anyLaundering")).first().getDouble(0)
        val covered      = top.agg(sum("launderingTxns")).first().getLong(0).toDouble / totalLaundering
        println("%-16d %21.1f%% %21.1f%% %21.1f%%".format(perDay, txnPrecision * 100, accPrecision * 100, covered * 100))
      }
    } finally spark.stop()
  }
}
