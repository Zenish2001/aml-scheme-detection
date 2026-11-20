package aml.batch

import java.sql.Date
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Part 6: stress-tests the IBM results. Do they hold on each test day, not just overall? How much does the tail
  * flatter them? What exactly is the midnight pattern behind hourOfDay? And how does the model do on Bitcoin
  * payments against bank payments?
  *
  * Run it from the sbt shell, after Stage1Main:  batch/runMain aml.batch.RobustnessMain
  */
object RobustnessMain {

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML robustness")

    try {
      val scores = spark.read.parquet(Stage1Main.ScoresFolder)
      val test   = scores.filter(col("period") === "test").cache()
      val tail   = scores.filter(col("period") === "tail").cache()

      // 1. Each test day on its own
      println("\n1. Stage 1 on each test day:")
      println("%-12s %12s %11s %9s %16s %13s".format("day", "transactions", "laundering", "PR-AUC", "precision @100", "recall @100"))
      test.select("day").distinct().orderBy("day").collect().map(_.getAs[Date](0)).foreach { day =>
        val d = test.filter(col("day") === lit(day)).cache()
        val a = Stage1.atDailyBudget(d, 100)
        println("%-12s %,12d %,11d %9.3f %15.1f%% %12.1f%%".format(
          day, d.count(), a.laundering, Stage1.prAuc(d), a.precision * 100, a.recall * 100))
      }

      // 2. How much the tail flatters the results
      println(f"\n2. PR-AUC on the test days: ${Stage1.prAuc(test)}%.3f; on the tail: ${Stage1.prAuc(tail)}%.3f; " +
        f"on both together: ${Stage1.prAuc(test.unionByName(tail))}%.3f")

      // 3. The midnight pattern
      val all = spark.read.parquet(Stage1Main.GraphFolder)
        .withColumn("atMidnight", col("hourOfDay") === 0)
        .withColumn("exactlyMidnight", col("hourOfDay") === 0 && minute(col("ts")) === 0)
      println("\n3. The midnight pattern: share of payments in the hour after midnight UTC, and at exactly 00:00")
      all.groupBy(when(col("isLaundering") === 1, "laundering").otherwise("normal").as("payments"))
        .agg(
          count(lit(1)).as("transactions"),
          round(avg(when(col("atMidnight"), 100.0).otherwise(0.0)), 1).as("hour 0 %"),
          round(avg(when(col("exactlyMidnight"), 100.0).otherwise(0.0)), 1).as("at 00:00 %"))
        .orderBy("payments").show(truncate = false)

      val normal = all.filter(col("isLaundering") === 0)
        .withColumn("when", when(col("atMidnight"), "hour 0").otherwise("other hours"))
      println("Normal payments in the hour after midnight against other hours, by payment format (% of each):")
      normal.groupBy("when", "format").count()
        .withColumn("share", round(col("count") * 100.0 / sum("count").over(Window.partitionBy("when")), 1))
        .groupBy("format").pivot("when", Seq("hour 0", "other hours")).agg(first("share"))
        .na.fill(0.0).orderBy(desc("hour 0")).show(truncate = false)
      println("Normal payments that are self-transfers, or within one bank (%):")
      normal.groupBy("when").agg(
          round(avg(col("isSelfTransfer")) * 100, 1).as("self-transfer %"),
          round(avg(col("isSameBank")) * 100, 1).as("same bank %"))
        .orderBy("when").show(truncate = false)

      // 4. Bitcoin against bank payments, at the top 100 alerts of each test day
      val ranked = test.withColumn("rank", row_number().over(Window.partitionBy("day").orderBy(desc("score"))))
        .join(all.select("txnId", "isBitcoin"), Seq("txnId")).cache()
      println("4. Bitcoin against bank payments on the test days, with the top 100 alerts of each day:")
      ranked.groupBy(when(col("isBitcoin") === 1, "Bitcoin").otherwise("bank").as("rail"))
        .agg(
          count(lit(1)).as("transactions"),
          sum("isLaundering").as("laundering"),
          sum(when(col("rank") <= 100, 1).otherwise(0)).as("alerts"),
          sum(when(col("rank") <= 100 && col("isLaundering") === 1, 1).otherwise(0)).as("caught"))
        .withColumn("recall %", round(col("caught") * 100.0 / col("laundering"), 1))
        .orderBy("rail").show(truncate = false)
    } finally spark.stop()
  }
}
