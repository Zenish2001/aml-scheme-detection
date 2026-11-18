package aml.batch

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Part 4a: the Stage 1 model, which scores how likely each transaction is to be laundering.
  *
  * Time decides the split, never chance: the model learns from Sep 1-5, choices between models are made on
  * Sep 6 (validation), and results are reported on Sep 7-10 (test), days it has never seen. The tail after
  * Sep 10 is 59% laundering, so it's reported apart. The model never sees the label, the date or any ID.
  */
object Stage1 {

  val TrainEnd         = "2022-09-05"
  val ValidationDay    = "2022-09-06"
  val TestEnd          = "2022-09-10"
  val NegativeFraction = 0.05   // train on every laundering transaction, and 5% of the normal ones

  private val perAccount = Seq(
    "Sent24hCount", "Sent24hUsd", "Sent24hMaxUsd", "Sent24hParties",
    "Recv24hCount", "Recv24hUsd", "Recv24hMaxUsd", "Recv24hParties", "Currencies24h", "Formats24h")

  /** The 20 account features of Part 2: each of the 10 above, for the sender and for the receiver. */
  val accountFeatures: Seq[String] = for (role <- Seq("sender", "receiver"); f <- perAccount) yield role + f

  /** The numeric inputs: 34 features plus the amount in US dollars (the payment format is added separately). */
  def numericFeatures(includeHour: Boolean): Seq[String] = {
    val all = (accountFeatures ++ TransactionFeatures.columns ++ GraphFeatures.columns :+ "amountPaidUsd").distinct
    if (includeHour) all else all.filterNot(_ == "hourOfDay")
  }

  /** train, validation, test or tail, by day. */
  def period(day: Column): Column =
    when(day <= to_date(lit(TrainEnd)), "train")
      .when(day === to_date(lit(ValidationDay)), "validation")
      .when(day <= to_date(lit(TestEnd)), "test")
      .otherwise("tail")

  def withLabel(df: DataFrame): DataFrame = df.withColumn("label", col("isLaundering").cast("double"))

  /** Every laundering row, and a NegativeFraction sample of the normal ones. Training on 0.1% positives would
    * mostly teach the model to say "normal"; the sample keeps the ranking it learns, and trains 20 times faster.
    */
  def trainingSample(train: DataFrame): DataFrame =
    train.stat.sampleBy("isLaundering", Map(0 -> NegativeFraction, 1 -> 1.0), seed = 42)

  /** Gradient-boosted trees: many small decision trees, each correcting the ones before. */
  def gbt(iterations: Int = 60): GBTClassifier =
    new GBTClassifier().setLabelCol("label").setFeaturesCol("features")
      .setMaxIter(iterations).setMaxDepth(5).setStepSize(0.1).setSubsamplingRate(0.8).setSeed(42)

  /** A simple straight-line model, as a baseline. */
  def logistic: LogisticRegression =
    new LogisticRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(100).setRegParam(0.001)

  /** Payment format as one-hot columns, everything into one feature vector, then the classifier. */
  def pipeline(numeric: Seq[String], classifier: PipelineStage): Pipeline = {
    val indexer   = new StringIndexer().setInputCol("format").setOutputCol("formatIndex").setHandleInvalid("keep")
    val encoder   = new OneHotEncoder().setInputCols(Array("formatIndex")).setOutputCols(Array("formatVec"))
    val assembler = new VectorAssembler().setInputCols((numeric :+ "formatVec").toArray).setOutputCol("features")
    new Pipeline().setStages(Array[PipelineStage](indexer, encoder, assembler, classifier))
  }

  /** Adds `score`, the model's probability of laundering, from 0 to 1. */
  def score(model: PipelineModel, df: DataFrame): DataFrame =
    model.transform(df)
      .withColumn("score", vector_to_array(col("probability")).getItem(1))
      .drop("features", "rawPrediction", "probability", "prediction", "formatIndex", "formatVec")

  /** Area under the precision-recall curve: 1 is perfect, and random guessing scores the share of laundering. */
  def prAuc(scored: DataFrame): Double =
    new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("score").setMetricName("areaUnderPR")
      .evaluate(withLabel(scored))

  /** How many alerts were raised, how many were laundering, and how much laundering there was in all. */
  final case class Alerts(alerts: Long, caught: Long, laundering: Long) {
    def recall: Double    = if (laundering == 0) 0.0 else caught.toDouble / laundering
    def precision: Double = if (alerts == 0) 0.0 else caught.toDouble / alerts
  }

  private def launderingIn(df: DataFrame): Long = df.filter(col("isLaundering") === 1).count()

  /** The `perDay` highest-scored transactions of each day become alerts: an analyst team's daily budget. */
  def atDailyBudget(scored: DataFrame, perDay: Int): Alerts = {
    val ranked = scored.withColumn("rank", row_number().over(Window.partitionBy("day").orderBy(desc("score"))))
    val alerts = ranked.filter(col("rank") <= perDay)
    Alerts(alerts.count(), launderingIn(alerts), launderingIn(scored))
  }

  /** The `n` highest-scored transactions overall become alerts. */
  def topN(scored: DataFrame, n: Long): Alerts = {
    val alerts = scored.orderBy(desc("score")).limit(n.toInt)
    Alerts(alerts.count(), launderingIn(alerts), launderingIn(scored))
  }

  /** Every transaction a rule matches becomes an alert. */
  def ofRule(df: DataFrame, rule: Column): Alerts = {
    val alerts = df.filter(rule)
    Alerts(alerts.count(), launderingIn(alerts), launderingIn(df))
  }

  /** Simple hand-written rules, built from the strongest signals in Parts 2 and 3, to compare with the model. */
  val rules: Seq[(String, Column)] = Seq(
    "New payee, receiver sent nothing in 24h" ->
      (col("pairPayments7d") === 0 && col("receiverSent24hCount") === 0 && col("isSelfTransfer") === 0),
    "New payee and over $5,000" ->
      (col("pairPayments7d") === 0 && col("amountPaidUsd") > 5000),
    "A network shape: loop or go-between" ->
      (col("reciprocal7d") > 0 || col("cyclePaths7d") > 0 || col("scatterGatherPaths7d") > 0))

  /** How much each input helped a GBT model, largest first. */
  def importances(model: PipelineModel, numeric: Seq[String]): Seq[(String, Double)] = {
    val formats = model.stages(0).asInstanceOf[StringIndexerModel].labelsArray(0).toSeq.map(f => s"format=$f")
    val values  = model.stages.last.asInstanceOf[GBTClassificationModel].featureImportances.toArray.toSeq
    (numeric ++ formats).zip(values).sortBy { case (_, v) => -v }
  }
}
