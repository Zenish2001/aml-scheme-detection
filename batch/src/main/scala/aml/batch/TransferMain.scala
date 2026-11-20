package aml.batch

import aml.common.RuleParser
import java.io.File
import java.sql.Date
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Part 6b: does the Stage 1 model work on data it has never seen?
  *
  * Runs the whole pipeline (checks, labels, exchange rates, 34 features) on another IBM dataset, LI-Small by
  * default, where laundering is rarer than in HI-Small. Then it scores the new data's later days with the model
  * trained on HI-Small, without retraining, and compares it with a model trained on the new data's own first
  * days, and with the rules in rules/aml.rules.
  *
  * Download the dataset's two files from Kaggle into data/raw first, then run it from the sbt shell:
  *   batch/runMain aml.batch.TransferMain              (LI-Small)
  *   batch/runMain aml.batch.TransferMain HI-Medium    (any IBM dataset, by its file name)
  * The features are saved, so a second run starts straight away.
  */
object TransferMain {

  val FullDay      = 100000L   // days with fewer transactions are the thin tail after the main days
  val TrainDays    = 5         // the new data's own model learns from its first 5 full days
  val DailyBudgets = Seq(25, 50, 100, 250)

  /** The full days, in order, split into the first `trainDays` to learn from and the rest to test on. */
  def splitDays(perDay: Seq[(Date, Long)], fullDay: Long, trainDays: Int): (Seq[Date], Seq[Date]) =
    perDay.filter(_._2 >= fullDay).map(_._1).sortBy(_.getTime).splitAt(trainDays)

  def main(args: Array[String]): Unit = {
    val name     = args.headOption.getOrElse("LI-Small")
    val raw      = s"data/raw/${name}_Trans.csv"
    val patterns = s"data/raw/${name}_Patterns.txt"
    val folder   = s"data/clean/${name.toLowerCase}"

    if (!new File(raw).exists) println(s"\n$raw isn't there: download it from Kaggle into data/raw first")
    else {
      val spark = LocalSpark.session(s"AML transfer to $name")
      try run(spark, name, raw, patterns, folder) finally spark.stop()
    }
  }

  private def done(folder: String): Boolean = new File(s"$folder/_SUCCESS").exists

  /** Checks, labels and exchange rates, as IngestMain does, then the account and transaction features. */
  private def buildFeatures(spark: SparkSession, raw: String, patterns: String): DataFrame = {
    val txns     = Ingest.readTransactions(spark, raw).cache()
    val valid    = txns.filter(Ingest.isValid).cache()
    val rejected = txns.count() - valid.count()
    println(f"Rows rejected by the checks: $rejected%,d")

    val labelled =
      if (new File(patterns).exists) Labels.withSchemes(valid, Labels.attemptTransactions(spark, Labels.readAttempts(patterns)))
      else valid.withColumn("scheme", when(col("isLaundering") === 1, lit("UNASSIGNED")).otherwise(lit(null).cast("string")))

    val pairs  = Rates.pairSummary(Rates.impliedRates(labelled)).collect().toSeq
    val usdPer = Rates.usdPerUnit(pairs.map(r =>
      Rates.PairRate(r.getAs[String]("from"), r.getAs[String]("to"), r.getAs[Double]("weightedRate"))))
    val euro    = usdPer.getOrElse("Euro", 0.0)
    val bitcoin = usdPer.getOrElse("Bitcoin", 0.0)
    println(f"Currencies with a US-dollar rate: ${usdPer.size}%d (Euro $euro%.4f dollars, Bitcoin $bitcoin%,.0f)")

    val inUsd  = Rates.withUsd(labelled, usdPer).withColumn("txnId", monotonically_increasing_id())
    val last24 = Features.last24Hours(Features.hourlyActivity(inUsd))
    TransactionFeatures.addAll(Features.withAccountFeatures(inUsd, last24))
  }

  private def run(spark: SparkSession, name: String, raw: String, patterns: String, folder: String): Unit = {
    // 1. Build the features once, in two saved steps, as for HI-Small
    if (!done(s"$folder/features")) {
      println(s"\nBuilding the account and transaction features for $name (a few minutes)...")
      buildFeatures(spark, raw, patterns).write.mode("overwrite").partitionBy("day").parquet(s"$folder/features")
    }
    if (!done(s"$folder/features_graph")) {
      println(s"Building the graph features for $name (a few minutes)...")
      val featured = spark.read.parquet(s"$folder/features").cache()
      GraphFeatures.withGraphFeatures(featured, GraphFeatures.hubs(featured))
        .write.mode("overwrite").partitionBy("day").parquet(s"$folder/features_graph")
    }

    // 2. The new data, day by day
    val all        = spark.read.parquet(s"$folder/features_graph").cache()
    val rows       = all.count()
    val laundering = all.filter(col("isLaundering") === 1).count()
    val empty      = all.filter(Stage1.numericFeatures(includeHour = false).map(c => col(c).isNull).reduce(_ || _)).count()
    println(f"\n$name: $rows%,d transactions, $laundering%,d laundering (1 in ${rows / math.max(laundering, 1L)}%,d), " +
      f"$empty%,d with an empty feature (expected 0). HI-Small: 1 in 981.")

    val perDay = all.groupBy("day").agg(count(lit(1)).as("n"), sum("isLaundering").as("laundering")).orderBy("day").collect()
    println("%-12s %12s %11s".format("day", "transactions", "laundering"))
    perDay.foreach(r => println("%-12s %,12d %,11d".format(r.getAs[Date]("day"), r.getAs[Long]("n"), r.getAs[Long]("laundering"))))
    val (trainDays, testDays) = splitDays(perDay.map(r => r.getAs[Date]("day") -> r.getAs[Long]("n")).toSeq, FullDay, TrainDays)
    if (testDays.isEmpty) { println(s"Fewer than ${TrainDays + 1} full days, so there's nothing to test on"); return }
    println(s"Learning days for its own model: ${trainDays.mkString(", ")}. Test days: ${testDays.mkString(", ")}")

    // 3. The HI-Small model, without retraining, against a model trained on the new data's own first days
    val test        = all.filter(col("day").isin(testDays: _*)).cache()
    val testRows    = test.count()
    val baseline    = test.filter(col("isLaundering") === 1).count().toDouble / testRows
    val transferred = Stage1.score(PipelineModel.load(Stage1Main.ModelFolder), test).cache()
    println("\nTraining a model on the new data's own first days, with the same settings (a few minutes)...")
    val sample = Stage1.trainingSample(Stage1.withLabel(all.filter(col("day").isin(trainDays: _*)))).cache()
    val own    = Stage1.pipeline(Stage1.numericFeatures(includeHour = false), Stage1.gbt()).fit(sample)
    val ownScored = Stage1.score(own, test).cache()

    println(f"\nOn $name's test days: PR-AUC (random guessing = $baseline%.4f), and precision of the top alerts each day")
    println("%-34s %8s %8s %8s %8s %8s".format("model", "PR-AUC", "@25", "@50", "@100", "@250"))
    Seq("HI-Small model, not retrained" -> transferred, s"Trained on $name's first days" -> ownScored).foreach { case (label, scored) =>
      val p = DailyBudgets.map(n => Stage1.atDailyBudget(scored, n).precision * 100)
      println("%-34s %8.3f %7.1f%% %7.1f%% %7.1f%% %7.1f%%".format(label, Stage1.prAuc(scored), p(0), p(1), p(2), p(3)))
    }
    println("%-34s %8.3f %7.1f%% %7.1f%% %7.1f%% %7.1f%%".format("For reference: HI-Small's own test", 0.269, 93.0, 92.0, 84.5, 47.7))

    // 4. The rules, which learn nothing from the data
    RuleParser.parseFile(RulesMain.RulesFile) match {
      case Left(error) => println(s"\n${RulesMain.RulesFile} has a mistake, at $error")
      case Right(rules) =>
        println(s"\nThe rules on $name's test days, and the HI-Small model given as many alerts:")
        println("%-46s %10s %9s %12s %12s".format("rule", "alerts/day", "recall", "precision", "model recall"))
        rules.foreach { rule =>
          val r = Stage1.ofRule(test, RuleColumns.toColumn(rule.condition))
          val m = Stage1.topN(transferred, r.alerts)
          val shortName = if (rule.name.length > 46) rule.name.take(45) + "\u2026" else rule.name
          println("%-46s %,10d %8.1f%% %11.2f%% %11.1f%%".format(
            shortName, r.alerts / testDays.size, r.recall * 100, r.precision * 100, m.recall * 100))
        }
    }

    // 5. Save the HI-Small model's scores on the new data
    transferred.select("txnId", "day", "isLaundering", "scheme", "score").write.mode("overwrite").parquet(s"$folder/scores")
    println(s"\nSaved the HI-Small model's scores on $name's test days to $folder/scores")
  }
}
