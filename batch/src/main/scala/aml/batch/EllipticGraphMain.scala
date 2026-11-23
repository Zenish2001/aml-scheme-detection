package aml.batch

import org.apache.spark.sql.functions._

/** Part 8a: do our own graph features help on real data? Trains the same random forest on Elliptic with and without
  * the four graph features from EllipticGraph, learning from time steps 1-34 and testing on 35-49 as before, and
  * compares illicit-class PR-AUC and F1. The proposal's target: our features raise PR-AUC by at least 10% over
  * Elliptic's own features alone.
  *
  * Run it from the sbt shell, after EllipticMain:  batch/runMain aml.batch.EllipticGraphMain
  */
object EllipticGraphMain {

  val Target = 10.0   // percent

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("Elliptic graph features")

    try {
      import EllipticModel._
      val txns  = labelled(spark.read.parquet(EllipticModelMain.TransactionsFolder))
      val edges = spark.read.parquet(s"${EllipticMain.CleanFolder}/edges")
      val data  = EllipticGraph.withGraph(txns, edges).cache()
      val train = data.filter(col("timeStep") <= TrainEnd).cache()
      val test  = data.filter(col("timeStep") > TrainEnd).cache()

      // 1. What the graph features look like, on the learning steps
      println("\nOur graph features on the learning steps (1-34): averages for illicit and licit transactions")
      train.groupBy(when(col("label") === 1.0, "illicit").otherwise("licit").as("class"))
        .agg(count(lit(1)).as("transactions"), EllipticGraph.columns.map(c => round(avg(c), 2).as(c)): _*)
        .orderBy("class").show(truncate = false)

      // 2. The same random forest, with and without them
      val g = EllipticGraph.columns
      val sets = Seq(
        "Local features" -> local,
        "Local + our graph features" -> (local ++ g),
        "All features (Elliptic's own)" -> all,
        "All + our graph features" -> (all ++ g))
      println("Learning from steps 1-34, testing on steps 35-49 (random forest):")
      println("%-34s %10s %11s %9s %9s".format("features", "PR-AUC", "precision", "recall", "F1"))
      val results = sets.map { case (name, features) =>
        val model  = pipeline(features, forest).fit(train)
        val scored = score(model, test).cache()
        val auc    = prAuc(scored)
        val f      = atThreshold(scored)
        println("%-34s %10.3f %10.1f%% %8.1f%% %9.3f".format(name, auc, f.precision * 100, f.recall * 100, f.f1))
        name -> (auc, model, features)
      }.toMap

      // 3. The proposal's criterion, and where the graph features rank
      def change(base: String, plus: String) = (results(plus)._1 / results(base)._1 - 1) * 100
      val local2 = change("Local features", "Local + our graph features")
      val all2   = change("All features (Elliptic's own)", "All + our graph features")
      println(f"\nAdding our graph features changes PR-AUC by $local2%+.1f%% on the local features, and $all2%+.1f%% on all of Elliptic's own")
      val verdict = if (all2 >= Target) "met" else "not met"
      println(f"The proposal's target, at least +$Target%.0f%% over Elliptic's own features: $verdict")

      val (_, model, features) = results("All + our graph features")
      val ranked = importances(model, features).zipWithIndex
      println(s"\nWhere our graph features rank among the ${features.size} inputs of that model, by importance:")
      ranked.filter { case ((n, _), _) => g.contains(n) }.foreach { case ((n, v), i) =>
        println("  %-10s rank %3d, importance %.4f".format(n, i + 1, v))
      }
    } finally spark.stop()
  }
}
