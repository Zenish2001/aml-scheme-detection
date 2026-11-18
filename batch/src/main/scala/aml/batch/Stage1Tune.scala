package aml.batch

import org.apache.spark.sql.functions._

/** Part 4a.2: tries four versions of the Stage 1 model, with more normal transactions to learn from and bigger
  * trees, and compares them on the validation day (Sep 6) only, so the test days stay unseen.
  *
  * Run it from the sbt shell:  batch/runMain aml.batch.Stage1Tune   (about 15-20 minutes)
  */
object Stage1Tune {

  /** (name, share of normal transactions kept for training, tree depth, number of trees) */
  val variants: Seq[(String, Double, Int, Int)] = Seq(
    ("5% normal, depth 5, 60 trees", 0.05, 5, 60),
    ("20% normal, depth 5, 60 trees", 0.20, 5, 60),
    ("5% normal, depth 7, 120 trees", 0.05, 7, 120),
    ("20% normal, depth 7, 120 trees", 0.20, 7, 120))

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML stage 1 tuning")

    try {
      val all        = Stage1.withLabel(spark.read.parquet(Stage1Main.GraphFolder)).withColumn("period", Stage1.period(col("day")))
      val train      = all.filter(col("period") === "train").cache()
      val validation = all.filter(col("period") === "validation").cache()
      val numeric    = Stage1.numericFeatures(includeHour = false)
      println(f"\nTraining days: ${train.count()}%,d transactions. Validation day: ${validation.count()}%,d.")

      println("\nOn the validation day (Sep 6): PR-AUC, and precision of the top 25, 50, 100 and 250 alerts")
      println("%-32s %10s %8s %8s %8s %8s %10s %8s".format("model", "rows", "PR-AUC", "@25", "@50", "@100", "@250", "minutes"))
      variants.foreach { case (name, fraction, depth, trees) =>
        val started = System.nanoTime()
        val sample  = Stage1.trainingSample(train, fraction).cache()
        val model   = Stage1.pipeline(numeric, Stage1.gbt(trees, depth)).fit(sample)
        val scored  = Stage1.score(model, validation).cache()
        val precisions = Seq(25, 50, 100, 250).map(n => Stage1.atDailyBudget(scored, n).precision * 100)
        val minutes = (System.nanoTime() - started) / 6e10
        println("%-32s %,10d %8.4f %7.1f%% %7.1f%% %7.1f%% %9.1f%% %8.1f".format(
          name, sample.count(), Stage1.prAuc(scored), precisions.head, precisions(1), precisions(2), precisions(3), minutes))
        sample.unpersist()
        scored.unpersist()
      }
    } finally spark.stop()
  }
}
