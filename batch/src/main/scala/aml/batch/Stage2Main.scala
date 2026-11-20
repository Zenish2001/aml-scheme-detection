package aml.batch

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._

/** Part 5: trains the Stage 2 models on Sep 1-5, chooses on Sep 6, and reports on Sep 7-10: accuracy and macro F1
  * against always guessing the commonest scheme, each scheme's results, the confusion between schemes, the
  * inputs that matter, the tail, the alerts Stage 1 caught, and what the unlisted laundering looks like.
  *
  * Run it from the sbt shell, after Stage1Main and ReasonsMain:  batch/runMain aml.batch.Stage2Main
  */
object Stage2Main {

  val ModelFolder       = "data/models/stage2"
  val PredictionsFolder = "data/clean/scheme_predictions"

  private val short = Map(
    "FAN-OUT" -> "F-OUT", "FAN-IN" -> "F-IN", "CYCLE" -> "CYCLE", "SCATTER-GATHER" -> "S-G",
    "GATHER-SCATTER" -> "G-S", "STACK" -> "STACK", "BIPARTITE" -> "BIPART", "RANDOM" -> "RANDOM")

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML stage 2")

    try {
      // 1. The listed laundering, by scheme and period
      val all    = spark.read.parquet(Stage1Main.GraphFolder).withColumn("period", Stage1.period(col("day")))
      val listed = Stage2.withSchemeLabel(Stage2.listed(all)).cache()
      println("\nListed laundering transactions, by scheme and period:")
      listed.groupBy("scheme").pivot("period", Seq("train", "validation", "test", "tail")).count()
        .na.fill(0).orderBy(desc("train")).show(truncate = false)

      val train      = Stage2.withClassWeights(listed.filter(col("period") === "train")).cache()
      val validation = listed.filter(col("period") === "validation").cache()
      val test       = listed.filter(col("period") === "test").cache()
      val tail       = listed.filter(col("period") === "tail").cache()
      val numeric    = Stage1.numericFeatures(includeHour = false)

      // 2. Three models, chosen on the validation day by macro F1
      val candidates: Seq[(String, PipelineModel)] = Seq(
        "Random forest" -> Stage1.pipeline(numeric, Stage2.forest(balanced = false)).fit(train),
        "Random forest, balanced" -> Stage1.pipeline(numeric, Stage2.forest(balanced = true)).fit(train),
        "Logistic regression" -> Stage1.pipeline(numeric, Stage2.logistic).fit(train))
      val results = candidates.map { case (name, model) =>
        (name, model,
          SchemeMetrics.of(Stage2.confusion(Stage2.predict(model, validation))),
          SchemeMetrics.of(Stage2.confusion(Stage2.predict(model, test))))
      }

      val trainCounts  = train.groupBy("scheme").count().collect().map(r => r.getString(0) -> r.getLong(1)).toMap
      val commonest    = trainCounts.maxBy(_._2)._1
      val testRows     = test.count()
      val guessingRate = test.filter(col("scheme") === commonest).count().toDouble / testRows
      println(f"Always guessing the commonest scheme ($commonest) would be right ${guessingRate * 100}%.1f%% of the time on the test days.")
      println("%-26s %12s %12s %12s %12s".format("model", "valid. acc.", "valid. F1", "test acc.", "test F1"))
      results.foreach { case (name, _, v, t) =>
        println("%-26s %11.1f%% %12.3f %11.1f%% %12.3f".format(name, v.accuracy * 100, v.macroF1, t.accuracy * 100, t.macroF1))
      }
      val (bestName, best, _, bestTest) = results.maxBy { case (_, _, v, _) => v.macroF1 }
      println(s"\nChosen on the validation day: $bestName")

      // 3. Each scheme, on the test days
      println(f"\n$bestName on the test days (Sep 7-10): $testRows%,d transactions, accuracy ${bestTest.accuracy * 100}%.1f%%, macro F1 ${bestTest.macroF1}%.3f")
      println("%-16s %9s %11s %9s %7s".format("scheme", "support", "precision", "recall", "F1"))
      bestTest.perScheme.sortBy(-_.support).foreach { s =>
        println("%-16s %,9d %10.1f%% %8.1f%% %7.3f".format(s.scheme, s.support, s.precision * 100, s.recall * 100, s.f1))
      }

      // 4. Which schemes get mistaken for which
      val confusion = Stage2.confusion(Stage2.predict(best, test))
      println("\nConfusion on the test days: rows are the true scheme, columns what the model said")
      println("%-16s".format("") + Stage2.Schemes.map(s => "%8s".format(short(s))).mkString)
      Stage2.Schemes.foreach { truth =>
        println("%-16s".format(truth) + Stage2.Schemes.map(guess => "%8d".format(confusion.getOrElse((truth, guess), 0L))).mkString)
      }

      // 5. What the forest relies on
      val (forestName, forestModel) = candidates.find(_._1 == bestName).filter(_._1.startsWith("Random"))
        .getOrElse(candidates(1))
      println(s"\nThe 12 most important inputs of $forestName:")
      Stage2.importances(forestModel, numeric).take(12).foreach { case (n, v) => println("  %-30s %6.3f".format(n, v)) }

      // 6. The tail, and the laundering Stage 1 caught
      val tailMetrics = SchemeMetrics.of(Stage2.confusion(Stage2.predict(best, tail)))
      println(f"\nOn the tail (Sep 11-18): accuracy ${tailMetrics.accuracy * 100}%.1f%%, macro F1 ${tailMetrics.macroF1}%.3f")
      val caught = Stage2.listed(spark.read.parquet(ReasonsMain.AlertsFolder).select("txnId", "isLaundering", "scheme"))
        .select("txnId").join(test, Seq("txnId"))
      val caughtRows    = caught.count()
      val caughtMetrics = SchemeMetrics.of(Stage2.confusion(Stage2.predict(best, caught)))
      println(f"On the $caughtRows%,d listed laundering transactions among Stage 1's top 100 alerts a day: " +
        f"accuracy ${caughtMetrics.accuracy * 100}%.1f%%, macro F1 ${caughtMetrics.macroF1}%.3f")

      // 7. The laundering IBM didn't list
      val unlisted = all.filter(col("isLaundering") === 1 && col("scheme") === "UNASSIGNED")
      val guesses  = Stage2.predict(best, unlisted).cache()
      val unlistedRows = guesses.count()
      println(f"\nWhat the model thinks the $unlistedRows%,d unlisted (UNASSIGNED) laundering transactions are:")
      guesses.groupBy("predictedScheme").agg(
          count(lit(1)).as("transactions"),
          round(count(lit(1)) * 100.0 / unlistedRows, 1).as("share %"),
          round(avg("confidence"), 2).as("avg confidence"))
        .orderBy(desc("transactions")).show(truncate = false)

      // 8. Save the model, and its prediction for every laundering transaction
      best.write.overwrite().save(ModelFolder)
      Stage2.predict(best, all.filter(col("isLaundering") === 1))
        .select("txnId", "day", "period", "scheme", "predictedScheme", "confidence")
        .write.mode("overwrite").parquet(PredictionsFolder)
      println(s"Saved the model to $ModelFolder, and its predictions to $PredictionsFolder")
    } finally spark.stop()
  }
}
