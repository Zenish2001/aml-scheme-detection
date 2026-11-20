package aml.batch

import org.apache.spark.ml.{PipelineModel, PipelineStage}
import org.apache.spark.sql.functions._

/** Part 6: trains models on Elliptic's real Bitcoin transactions, chooses one without touching the test steps,
  * and reports on time steps 35-49: illicit F1 at a 0.5 threshold (the paper's measure), PR-AUC, an alert budget
  * per time step, each step on its own, before and after the dark market shut down, and which features matter.
  *
  * Run it from the sbt shell, after EllipticMain:  batch/runMain aml.batch.EllipticModelMain
  */
object EllipticModelMain {

  val TransactionsFolder = s"${EllipticMain.CleanFolder}/transactions"
  val ScoresFolder       = s"${EllipticMain.CleanFolder}/scores"
  val PerStepBudget      = 50

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("Elliptic models")

    try {
      import EllipticModel._
      val txns   = labelled(spark.read.parquet(TransactionsFolder)).cache()
      val fit    = txns.filter(col("timeStep") < ChooseFrom).cache()
      val choose = txns.filter(col("timeStep").between(ChooseFrom, TrainEnd)).cache()
      val train  = txns.filter(col("timeStep") <= TrainEnd).cache()
      val test   = txns.filter(col("timeStep") > TrainEnd).cache()

      println("\nLabelled transactions (illicit share):")
      Seq("Steps 1-29, to learn while choosing" -> fit, "Steps 30-34, to choose" -> choose,
          "Steps 1-34, to learn" -> train, "Steps 35-49, to test" -> test).foreach { case (name, df) =>
        val n       = df.count()
        val illicit = df.filter(col("label") === 1.0).count()
        println(f"  $name%-38s $n%,7d   ${illicit * 100.0 / n}%5.1f%% illicit")
      }

      val candidates: Seq[(String, Seq[String], () => PipelineStage)] = Seq(
        ("Logistic regression, local", local, () => logistic),
        ("Random forest, local", local, () => forest),
        ("Random forest, all", all, () => forest),
        ("GBT, all", all, () => gbt))

      // 1. Choose on steps 30-34, having learned from 1-29
      println("\nChoosing: learn from steps 1-29, compare on steps 30-34")
      println("%-28s %10s %10s".format("model", "PR-AUC", "F1 at 0.5"))
      val choices = candidates.map { case (name, features, make) =>
        val scored = score(pipeline(features, make()).fit(fit), choose).cache()
        val auc    = prAuc(scored)
        println("%-28s %10.3f %10.3f".format(name, auc, atThreshold(scored).f1))
        name -> auc
      }
      val chosen = choices.maxBy(_._2)._1
      println(s"Chosen on steps 30-34: $chosen")

      // 2. Every model, learned from 1-34 and tested on 35-49
      println("\nTesting: learn from steps 1-34, test on steps 35-49")
      println("%-28s %10s %11s %9s %9s".format("model", "PR-AUC", "precision", "recall", "F1"))
      val models: Map[String, (PipelineModel, Seq[String])] = candidates.map { case (name, features, make) =>
        val model  = pipeline(features, make()).fit(train)
        val scored = score(model, test).cache()
        val f      = atThreshold(scored)
        println("%-28s %10.3f %10.1f%% %8.1f%% %9.3f".format(name, prAuc(scored), f.precision * 100, f.recall * 100, f.f1))
        name -> (model, features)
      }.toMap

      // 3. The chosen model in detail
      val (best, bestFeatures) = models(chosen)
      val scored = score(best, test).cache()

      val before = atThreshold(scored.filter(col("timeStep") < ShutdownStep))
      val after  = atThreshold(scored.filter(col("timeStep") >= ShutdownStep))
      println(f"\n$chosen, before and after the dark market shut down (step $ShutdownStep):")
      println("%-22s %9s %9s %11s %9s %9s".format("", "illicit", "caught", "precision", "recall", "F1"))
      Seq(s"Steps 35-${ShutdownStep - 1}" -> before, s"Steps $ShutdownStep-49" -> after).foreach { case (name, f) =>
        println("%-22s %,9d %,9d %10.1f%% %8.1f%% %9.3f".format(name, f.illicit, f.caught, f.precision * 100, f.recall * 100, f.f1))
      }

      println("\nEach test step, at a 0.5 threshold:")
      println("%-6s %9s %9s %9s %9s".format("step", "illicit", "flagged", "caught", "recall"))
      byStep(scored).foreach { case (step, f) =>
        println("%-6d %,9d %,9d %,9d %8.1f%%".format(step, f.illicit, f.flagged, f.caught, f.recall * 100))
      }

      val budget = atStepBudget(scored, PerStepBudget)
      println(f"\nWith a budget of $PerStepBudget%d alerts per time step: ${budget.flagged}%,d alerts, " +
        f"precision ${budget.precision * 100}%.1f%%, recall ${budget.recall * 100}%.1f%%")

      val forestName = if (chosen.startsWith("Random")) chosen else "Random forest, all"
      val (forestModel, forestFeatures) = models(forestName)
      val ranked = importances(forestModel, forestFeatures)
      val aggShare = ranked.filter(_._1.startsWith("agg_")).map(_._2).sum
      println(f"\nThe 10 most important features of $forestName (aggregated features carry ${aggShare * 100}%.0f%% of the importance):")
      ranked.take(10).foreach { case (n, v) => println("  %-12s %6.3f".format(n, v)) }

      // 4. Save the chosen model's scores for the test steps
      scored.select("txId", "timeStep", "label", "score").write.mode("overwrite").parquet(ScoresFolder)
      println(s"\nSaved the scores of ${scored.count()} test transactions to $ScoresFolder (features used: ${bestFeatures.size})")
    } finally spark.stop()
  }
}
