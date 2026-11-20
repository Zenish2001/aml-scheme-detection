package aml.batch

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Part 6: the methods on real data, Elliptic's Bitcoin transactions.
  *
  * Time decides the split, as for IBM and as in the Elliptic paper: models learn from time steps 1-34 and are
  * tested on 35-49. To choose between models without touching the test steps, they first learn from 1-29 and
  * are compared on 30-34. Only labelled transactions (illicit or licit) are used; the time step itself is never
  * a feature.
  */
object EllipticModel {

  val TrainEnd     = 34   // learn from steps 1-34, test on 35-49
  val ChooseFrom   = 30   // to choose: learn from 1-29, compare on 30-34
  val ShutdownStep = 43   // from here, a dark market had shut down and illicit activity nearly vanishes

  /** The 93 local features (about the transaction itself) and the 72 aggregated ones (about its neighbours). */
  val local: Seq[String]      = (1 to 93).map(i => s"local_$i")
  val aggregated: Seq[String] = (1 to 72).map(i => s"agg_$i")
  val all: Seq[String]        = local ++ aggregated

  /** Illicit and licit transactions only, with label 1.0 for illicit. */
  def labelled(df: DataFrame): DataFrame =
    df.filter(col("isIllicit").isNotNull).withColumn("label", col("isIllicit").cast("double"))

  def pipeline(features: Seq[String], classifier: PipelineStage): Pipeline =
    new Pipeline().setStages(Array[PipelineStage](
      new VectorAssembler().setInputCols(features.toArray).setOutputCol("features"), classifier))

  def forest: RandomForestClassifier =
    new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setNumTrees(100).setMaxDepth(12).setSeed(42)

  def gbt: GBTClassifier =
    new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(100).setMaxDepth(5).setSeed(42)

  def logistic: LogisticRegression =
    new LogisticRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(200).setRegParam(0.001)

  /** Adds `score`, the model's probability that a transaction is illicit. */
  def score(model: PipelineModel, df: DataFrame): DataFrame =
    model.transform(df).withColumn("score", vector_to_array(col("probability")).getItem(1))
      .drop("features", "rawPrediction", "probability", "prediction")

  def prAuc(scored: DataFrame): Double =
    new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("score").setMetricName("areaUnderPR")
      .evaluate(scored)

  /** How many transactions were flagged, how many of them were illicit, and how many illicit there were in all. */
  final case class Flags(flagged: Long, caught: Long, illicit: Long) {
    def precision: Double = if (flagged == 0) 0.0 else caught.toDouble / flagged
    def recall: Double    = if (illicit == 0) 0.0 else caught.toDouble / illicit
    def f1: Double        = if (precision + recall == 0) 0.0 else 2 * precision * recall / (precision + recall)
  }

  private def counted(df: DataFrame, flagged: org.apache.spark.sql.Column): Flags = {
    val row = df.agg(
      coalesce(sum(when(flagged, 1).otherwise(0)), lit(0L)),
      coalesce(sum(when(flagged && col("label") === 1.0, 1).otherwise(0)), lit(0L)),
      coalesce(sum(when(col("label") === 1.0, 1).otherwise(0)), lit(0L))).first()
    Flags(row.getLong(0), row.getLong(1), row.getLong(2))
  }

  /** Every transaction scored at or above the threshold is flagged, as in the paper's illicit F1. */
  def atThreshold(scored: DataFrame, threshold: Double = 0.5): Flags = counted(scored, col("score") >= threshold)

  /** The `perStep` highest-scored transactions of each time step are flagged: a daily alert budget, as for IBM. */
  def atStepBudget(scored: DataFrame, perStep: Int): Flags =
    counted(scored.withColumn("rank", row_number().over(Window.partitionBy("timeStep").orderBy(desc("score")))),
      col("rank") <= perStep)

  /** Flags at the threshold, for each time step. */
  def byStep(scored: DataFrame, threshold: Double = 0.5): Seq[(Int, Flags)] =
    scored.groupBy("timeStep").agg(
        sum(when(col("score") >= threshold, 1).otherwise(0)).as("flagged"),
        sum(when(col("score") >= threshold && col("label") === 1.0, 1).otherwise(0)).as("caught"),
        sum(when(col("label") === 1.0, 1).otherwise(0)).as("illicit"))
      .orderBy("timeStep").collect().toSeq
      .map(r => r.getInt(0) -> Flags(r.getLong(1), r.getLong(2), r.getLong(3)))

  /** How much each feature helped a random forest, largest first. */
  def importances(model: PipelineModel, features: Seq[String]): Seq[(String, Double)] =
    features.zip(model.stages.last.asInstanceOf[RandomForestClassificationModel].featureImportances.toArray)
      .sortBy { case (_, v) => -v }
}
