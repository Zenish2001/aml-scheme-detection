package aml.batch

import org.apache.spark.sql.functions._

/** Part 1c: loads Elliptic's real Bitcoin dataset, checks it against the published numbers, and saves it as
  * Parquet: the transactions (features and labels) by time step, and the payments between them.
  *
  * Run it from the sbt shell:  batch/runMain aml.batch.EllipticMain
  */
object EllipticMain {

  val RawFolder   = "data/raw/elliptic"
  val CleanFolder = "data/clean/elliptic"

  // Published by Weber et al. (2019), who released the dataset
  val ExpectedTransactions = 203769L
  val ExpectedEdges        = 234355L
  val ExpectedIllicit      = 4545L
  val ExpectedLicit        = 42019L
  val ExpectedUnknown      = ExpectedTransactions - ExpectedIllicit - ExpectedLicit

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("Elliptic ingest")

    try {
      // 1. Transactions: features plus labels
      val features = Elliptic.readFeatures(spark, s"$RawFolder/elliptic_txs_features.csv")
      val classes  = Elliptic.readClasses(spark, s"$RawFolder/elliptic_txs_classes.csv").cache()
      val txns     = Elliptic.transactions(features, classes).cache()

      val txnCount      = txns.count()
      val distinctIds   = txns.select("txId").distinct().count()
      val invalid       = txns.filter(!Elliptic.isValid).count()
      val noClass       = txns.join(classes.select("txId"), Seq("txId"), "left_anti").count()
      val classNoTxn    = classes.join(txns.select("txId"), Seq("txId"), "left_anti").count()

      println()
      println(f"Transactions:                         $txnCount%,d   (expected $ExpectedTransactions%,d)")
      println(f"Distinct transaction IDs:             $distinctIds%,d   (expected $ExpectedTransactions%,d)")
      println(f"Rows with a value that didn't parse:  $invalid%,d   (expected 0)")
      println(f"Transactions missing from classes:    $noClass%,d   (expected 0)")
      println(f"Classes for unknown transactions:     $classNoTxn%,d   (expected 0)")

      println(f"\nLabels (expected illicit $ExpectedIllicit%,d, licit $ExpectedLicit%,d, unknown $ExpectedUnknown%,d):")
      txns.groupBy("label").count().orderBy("label").show()

      // 2. Payments between transactions
      val edges     = Elliptic.readEdges(spark, s"$RawFolder/elliptic_txs_edgelist.csv").cache()
      val edgeCount = edges.count()
      val steps     = txns.select("txId", "timeStep")
      val withSteps = edges
        .join(steps.withColumnRenamed("txId", "src").withColumnRenamed("timeStep", "srcStep"), Seq("src"), "left")
        .join(steps.withColumnRenamed("txId", "dst").withColumnRenamed("timeStep", "dstStep"), Seq("dst"), "left")
        .cache()
      val dangling  = withSteps.filter(col("srcStep").isNull || col("dstStep").isNull).count()
      val crossStep = withSteps.filter(col("srcStep") =!= col("dstStep")).count()

      println(f"Payments (edges):                     $edgeCount%,d   (expected $ExpectedEdges%,d)")
      println(f"Edges to a transaction that's absent: $dangling%,d   (expected 0)")
      println(f"Edges between different time steps:   $crossStep%,d   (expected 0: each time step is its own graph)")

      // 3. How the labels spread over time
      println("\nTransactions per time step:")
      txns.groupBy("timeStep").agg(
          count(lit(1)).as("transactions"),
          sum(when(col("label") === Elliptic.Illicit, 1).otherwise(0)).as("illicit"),
          sum(when(col("label") === Elliptic.Licit, 1).otherwise(0)).as("licit"))
        .orderBy("timeStep").show(49)

      // 4. Save
      txns.write.mode("overwrite").partitionBy("timeStep").parquet(s"$CleanFolder/transactions")
      edges.write.mode("overwrite").parquet(s"$CleanFolder/edges")
      println(f"Saved $txnCount%,d transactions and $edgeCount%,d edges to $CleanFolder")
    } finally spark.stop()
  }
}
