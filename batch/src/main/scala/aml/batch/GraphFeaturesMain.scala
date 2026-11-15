package aml.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Part 3: adds the graph features to the transactions from FeaturesMain, checks them, shows how often each
  * appears for each scheme, and saves the result.
  *
  * Run it from the sbt shell, after FeaturesMain:  batch/runMain aml.batch.GraphFeaturesMain
  */
object GraphFeaturesMain {

  val FeaturesFolder = "data/clean/features"
  val GraphFolder    = "data/clean/features_graph"
  val SampleFraction = 0.0002   // about 1,000 transactions for the brute-force checks

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML graph features")

    try {
      // 1. Hubs, then the features
      val txns     = spark.read.parquet(FeaturesFolder).cache()
      val txnCount = txns.count()
      val hubs     = GraphFeatures.hubs(txns).cache()
      val hubCount = hubs.count()
      val hubTxns  = txns.join(hubs.withColumnRenamed("account", "senderAccount"), Seq("senderAccount"), "left_semi").select("txnId")
        .union(txns.join(hubs.withColumnRenamed("account", "receiverAccount"), Seq("receiverAccount"), "left_semi").select("txnId"))
        .distinct().count()

      val withGraph = GraphFeatures.withGraphFeatures(txns, hubs).cache()
      val rows      = withGraph.count()
      val nulls     = withGraph.filter(GraphFeatures.columns.map(c => col(c).isNull).reduce(_ || _)).count()

      println()
      println(f"Transactions:                         $txnCount%,d")
      println(f"Hubs (over ${GraphFeatures.HubDegree}%d counterparties):       $hubCount%,d")
      println(f"Transactions with a hub on one side:  $hubTxns%,d (${hubTxns * 100.0 / txnCount}%.1f%%)")
      println(f"Rows with graph features:             $rows%,d   (expected $txnCount%,d)")
      println(f"Rows with an empty graph feature:     $nulls%,d   (expected 0)")

      // 2. Brute-force checks on a sample, from the individual payments rather than hourly totals
      val sample = withGraph
        .sample(withReplacement = false, fraction = SampleFraction, seed = 42)
        .withColumn("id", monotonically_increasing_id())
        .select("id", "senderAccount", "receiverAccount", "hour", "reciprocal7d", "cyclePaths7d", "scatterGatherPaths7d")
        .cache()
      val payments = txns.filter(col("senderAccount") =!= col("receiverAccount"))
        .select(col("senderAccount").as("src"), col("receiverAccount").as("dst"), col("hour").as("ph"))
        .cache()
      println(f"\nBrute-force checks on ${sample.count()}%,d sampled transactions (mismatches, expected 0):")
      println(f"  reciprocal7d:          ${reciprocalMismatches(sample, payments)}%,d")
      println(f"  cyclePaths7d:          ${cycleMismatches(sample, payments, hubs)}%,d")
      println(f"  scatterGatherPaths7d:  ${scatterMismatches(sample, payments, hubs)}%,d")

      // 3. How often each feature is above zero, for normal transactions and each scheme
      println("\nShare of transactions with each feature above zero, by scheme (%):")
      withGraph
        .withColumn("group", coalesce(col("scheme"), lit("(normal)")))
        .groupBy("group").agg(
          count(lit(1)).as("transactions"),
          round(avg(when(col("reciprocal7d") > 0, 100.0).otherwise(0.0)), 1).as("reciprocal"),
          round(avg(when(col("cyclePaths7d") > 0, 100.0).otherwise(0.0)), 1).as("cycle"),
          round(avg(when(col("scatterGatherPaths7d") > 0, 100.0).otherwise(0.0)), 1).as("scatterGather"),
          round(avg(when(col("isBitcoin") > 0, 100.0).otherwise(0.0)), 1).as("bitcoin"),
          round(avg(when(col("senderRecvOtherRail24h") > 0, 100.0).otherwise(0.0)), 1).as("otherRail"))
        .orderBy(desc("transactions"))
        .show(truncate = false)

      // 4. Save
      withGraph.write.mode("overwrite").partitionBy("day").parquet(GraphFolder)
      println(f"Saved $rows%,d transactions with ${GraphFeatures.columns.size}%d graph features to $GraphFolder")
    } finally spark.stop()
  }

  private def inWindow(pastHour: String) = GraphFeatures.inWindow(col(pastHour), col("hour"))

  private def mismatches(sample: DataFrame, counted: DataFrame, feature: String): Long =
    sample.join(counted, Seq("id"), "left")
      .filter(coalesce(col("expected"), lit(0L)) =!= col(feature)).count()

  /** Payments from r back to s, one by one. */
  private def reciprocalMismatches(sample: DataFrame, payments: DataFrame): Long = {
    val counted = sample
      .join(payments, col("src") === col("receiverAccount") && col("dst") === col("senderAccount"))
      .filter(inWindow("ph"))
      .groupBy("id").agg(count(lit(1)).as("expected"))
    mismatches(sample, counted, "reciprocal7d")
  }

  /** Accounts x with a payment r -> x, then a later payment x -> s, found one payment at a time. */
  private def cycleMismatches(sample: DataFrame, payments: DataFrame, hubs: DataFrame): Long = {
    val first  = payments.select(col("src").as("receiverAccount"), col("dst").as("x"), col("ph").as("h1"))
    val second = payments.select(col("src").as("x"), col("dst").as("senderAccount"), col("ph").as("h2"))
    val counted = sample
      .filter(col("senderAccount") =!= col("receiverAccount"))
      .join(first, Seq("receiverAccount"))
      .filter(inWindow("h1"))
      .join(hubs.withColumnRenamed("account", "x"), Seq("x"), "left_anti")
      .join(second, Seq("x", "senderAccount"))
      .filter(inWindow("h2") && col("h1") < col("h2"))
      .groupBy("id").agg(countDistinct("x").as("expected"))
    mismatches(sample, counted, "cyclePaths7d")
  }

  /** Accounts x != s with payments p -> s, p -> x, then x -> r, for some non-hub p; 0 if s or r is a hub. */
  private def scatterMismatches(sample: DataFrame, payments: DataFrame, hubs: DataFrame): Long = {
    val toSender = payments.select(col("src").as("p"), col("dst").as("senderAccount"), col("ph").as("hp"))
    val toX      = payments.select(col("src").as("p"), col("dst").as("x"), col("ph").as("h1"))
    val toR      = payments.select(col("src").as("x"), col("dst").as("receiverAccount"), col("ph").as("h2"))
    val counted = sample
      .join(hubs.withColumnRenamed("account", "senderAccount"), Seq("senderAccount"), "left_anti")
      .join(hubs.withColumnRenamed("account", "receiverAccount"), Seq("receiverAccount"), "left_anti")
      .join(toSender, Seq("senderAccount"))
      .filter(inWindow("hp"))
      .join(hubs.withColumnRenamed("account", "p"), Seq("p"), "left_anti")
      .filter(col("p") =!= col("receiverAccount"))
      .join(toX, Seq("p"))
      .filter(inWindow("h1") && col("x") =!= col("senderAccount"))
      .join(hubs.withColumnRenamed("account", "x"), Seq("x"), "left_anti")
      .join(toR, Seq("x", "receiverAccount"))
      .filter(inWindow("h2") && col("h1") < col("h2"))
      .groupBy("id").agg(countDistinct("x").as("expected"))
    mismatches(sample, counted, "scatterGatherPaths7d")
  }
}
