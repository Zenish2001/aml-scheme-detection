package aml.batch

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Part 4a: trains the Stage 1 models on Sep 1-5, chooses on Sep 6, and reports on Sep 7-10: precision-recall,
  * alerts at daily budgets, simple rules against the model, and which features matter most.
  *
  * Run it from the sbt shell, after GraphFeaturesMain:  batch/runMain aml.batch.Stage1Main
  */
object Stage1Main {

  val GraphFolder  = "data/clean/features_graph"
  val ScoresFolder = "data/clean/scores"
  val ModelFolder  = "data/models/stage1"
  val DailyBudgets = Seq(25, 50, 100, 250)

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML stage 1")

    try {
      // 1. Split by time
      val all = Stage1.withLabel(spark.read.parquet(GraphFolder)).withColumn("period", Stage1.period(col("day")))
      println("\nTransactions and laundering by period:")
      all.groupBy("period").agg(count(lit(1)).as("transactions"), sum("isLaundering").as("laundering"))
        .orderBy(when(col("period") === "train", 1).when(col("period") === "validation", 2).when(col("period") === "test", 3).otherwise(4))
        .show()

      val sample     = Stage1.trainingSample(all.filter(col("period") === "train")).cache()
      val validation = all.filter(col("period") === "validation").cache()
      val test       = all.filter(col("period") === "test").cache()
      val tail       = all.filter(col("period") === "tail").cache()
      val baseline   = test.filter(col("isLaundering") === 1).count().toDouble / test.count()
      println(f"Training sample: ${sample.count()}%,d rows (every laundering transaction, and 5%% of the normal ones)")

      // 2. Train three models
      val withHour    = Stage1.numericFeatures(includeHour = true)
      val withoutHour = Stage1.numericFeatures(includeHour = false)
      println("Training: logistic regression, then two gradient-boosted tree models (a few minutes)...")
      val candidates: Seq[(String, PipelineModel, Seq[String])] = Seq(
        ("Logistic regression", Stage1.pipeline(withoutHour, Stage1.logistic).fit(sample), withoutHour),
        ("GBT, with hourOfDay", Stage1.pipeline(withHour, Stage1.gbt()).fit(sample), withHour),
        ("GBT, without hourOfDay", Stage1.pipeline(withoutHour, Stage1.gbt()).fit(sample), withoutHour))

      val results = candidates.map { case (name, model, numeric) =>
        (name, model, numeric, Stage1.prAuc(Stage1.score(model, validation)), Stage1.prAuc(Stage1.score(model, test)))
      }
      println(f"\nPR-AUC (1 = perfect; random guessing on the test days = $baseline%.4f):")
      println("%-26s %12s %12s".format("model", "validation", "test"))
      results.foreach { case (name, _, _, v, t) => println("%-26s %12.4f %12.4f".format(name, v, t)) }

      // The choice is made on the validation day, never on the test days
      val (bestName, best, bestNumeric, _, bestTest) = results.maxBy { case (_, _, _, v, _) => v }
      println(s"\nChosen on the validation day: $bestName")

      // 3. Alerts at daily budgets, on the test days
      val scoredTest = Stage1.score(best, test).cache()
      println(f"\n$bestName on the test days (Sep 7-10), PR-AUC $bestTest%.4f. Alerts at a daily budget:")
      println("%-18s %10s %10s %12s %10s".format("alerts per day", "alerts", "caught", "recall", "precision"))
      DailyBudgets.foreach { perDay =>
        val a = Stage1.atDailyBudget(scoredTest, perDay)
        println("%-18d %,10d %,10d %11.1f%% %9.1f%%".format(perDay, a.alerts, a.caught, a.recall * 100, a.precision * 100))
      }
      val tailAuc = Stage1.prAuc(Stage1.score(best, tail))
      println(f"On the tail (Sep 11-18), PR-AUC $tailAuc%.4f: 59%% of the tail is laundering, so this flatters the model")

      // 4. Simple rules against the model, at the same number of alerts
      println("\nRules against the model, on the test days, with the model given as many alerts as each rule:")
      println("%-42s %10s %12s %12s %12s".format("rule", "alerts", "rule recall", "model recall", "rule prec."))
      Stage1.rules.foreach { case (name, rule) =>
        val r = Stage1.ofRule(test, rule)
        val m = Stage1.topN(scoredTest, r.alerts)
        println("%-42s %,10d %11.1f%% %11.1f%% %11.2f%%".format(name, r.alerts, r.recall * 100, m.recall * 100, r.precision * 100))
      }

      // 5. What the trees rely on
      val (gbtName, gbtModel, gbtNumeric) = candidates.filter(_._1.startsWith("GBT")).find(_._1 == bestName)
        .getOrElse(candidates.last)
      println(s"\nThe 15 most important inputs of $gbtName:")
      Stage1.importances(gbtModel, gbtNumeric).take(15).foreach { case (n, v) => println("  %-30s %6.3f".format(n, v)) }

      // 6. Save the chosen model, and the scores of every transaction it didn't train on
      best.write.overwrite().save(ModelFolder)
      Stage1.score(best, all.filter(col("period") =!= "train"))
        .select("txnId", "day", "period", "isLaundering", "scheme", "score")
        .write.mode("overwrite").partitionBy("day").parquet(ScoresFolder)
      println(s"\nSaved the model to $ModelFolder, and the scores to $ScoresFolder")
    } finally spark.stop()
  }
}
