package aml.batch

import org.apache.spark.sql.functions._

/** Part 2: adds each transaction's account features (what its sender and receiver did in the 24 hours before it),
  * checks them, compares laundering with normal transactions, and saves the result.
  *
  * Run it from the sbt shell, after IngestMain:  batch/runMain aml.batch.FeaturesMain
  */
object FeaturesMain {

  val TxnsFolder     = "data/clean/txns"
  val FeaturesFolder = "data/clean/features"

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML account features")

    try {
      // 1. Build the features
      val txns     = spark.read.parquet(TxnsFolder)
      val txnCount = txns.count()

      val last24       = Features.last24Hours(Features.hourlyActivity(txns)).cache()
      val accountHours = last24.count()
      val accounts     = last24.select("account").distinct().count()

      val featured      = Features.withAccountFeatures(txns, last24).cache()
      val featuredCount = featured.count()
      val names         = Features.featureColumns(featured)
      val nulls         = featured.filter(names.map(c => col(c).isNull).reduce(_ || _)).count()

      println()
      println(f"Transactions:                     $txnCount%,d")
      println(f"Accounts:                         $accounts%,d")
      println(f"Account-hours with any activity:  $accountHours%,d")
      println(f"Rows with features:               $featuredCount%,d   (expected $txnCount%,d)")
      println(f"Features per transaction:         ${names.size}%d")
      println(f"Rows with an empty feature:       $nulls%,d   (expected 0)")

      // 2. Check one feature the slow, obvious way on a sample of about 1,000 transactions
      val sample = featured
        .sample(withReplacement = false, fraction = 0.0002, seed = 42)
        .withColumn("id", monotonically_increasing_id())
        .select("id", "senderAccount", "hour", "senderSent24hCount")
        .cache()
      val payments = featured.select(col("senderAccount"), col("hour").as("paymentHour"))
      val counted = sample
        .join(payments, Seq("senderAccount"))
        .filter(col("paymentHour").between(col("hour") - Features.WindowHours, col("hour") - 1))
        .groupBy("id").count()
      val mismatches = sample.join(counted, Seq("id"), "left")
        .filter(coalesce(col("count"), lit(0L)) =!= col("senderSent24hCount"))
        .count()
      println(f"Sampled transactions checked by brute force:  ${sample.count()}%,d, mismatches: $mismatches%,d   (expected 0)")

      // 3. How laundering transactions differ
      val means  = featured.groupBy("isLaundering").agg(avg(names.head).as(names.head), names.tail.map(c => avg(c).as(c)): _*).collect()
      val byFlag = means.map(row => row.getAs[Int]("isLaundering") -> row).toMap

      println("\nAverage of each feature, normal against laundering transactions:")
      println("%-28s %16s %16s %8s".format("feature", "normal", "laundering", "ratio"))
      names.foreach { c =>
        val normal     = byFlag(0).getAs[Double](c)
        val laundering = byFlag(1).getAs[Double](c)
        println("%-28s %,16.2f %,16.2f %8.2f".format(c, normal, laundering, laundering / normal))
      }

      // 4. Save
      featured.write.mode("overwrite").partitionBy("day").parquet(FeaturesFolder)
      println(f"\nSaved $featuredCount%,d transactions with ${names.size}%d account features to $FeaturesFolder")
    } finally spark.stop()
  }
}
