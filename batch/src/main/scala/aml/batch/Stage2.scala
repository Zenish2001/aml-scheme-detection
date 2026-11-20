package aml.batch

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Part 5: the Stage 2 model, which says which of the 8 schemes a laundering transaction belongs to.
  *
  * It learns from the laundering transactions IBM lists with their scheme, with the same time split as Stage 1:
  * Sep 1-5 to learn, Sep 6 to choose, Sep 7-10 to report, and the tail apart. The 1,968 laundering transactions
  * IBM didn't list (UNASSIGNED) are never used to learn; the program shows what the model thinks they are.
  */
object Stage2 {

  /** The 8 schemes, in a fixed order: a scheme's position is the label the model learns. */
  val Schemes: Seq[String] =
    Seq("FAN-OUT", "FAN-IN", "CYCLE", "SCATTER-GATHER", "GATHER-SCATTER", "STACK", "BIPARTITE", "RANDOM")

  /** Laundering transactions with one of the 8 schemes. */
  def listed(df: DataFrame): DataFrame =
    df.filter(col("isLaundering") === 1 && col("scheme").isin(Schemes: _*))

  /** Adds schemeLabel: the scheme's position in Schemes, from 0 to 7. */
  def withSchemeLabel(df: DataFrame): DataFrame =
    df.withColumn("schemeLabel", (array_position(array(Schemes.map(s => lit(s)): _*), col("scheme")) - 1).cast("double"))

  /** Adds weight, so that each scheme counts equally in training however many transactions it has. Weights
    * average 1: a scheme with half the average number of transactions gets weight 2.
    */
  def withClassWeights(train: DataFrame): DataFrame = {
    val counts  = train.groupBy("scheme").count()
    val rows    = train.count().toDouble
    val schemes = counts.count().toDouble
    train.join(counts, Seq("scheme")).withColumn("weight", lit(rows) / (lit(schemes) * col("count"))).drop("count")
  }

  /** A random forest: many deep decision trees, each trained on a random part of the data, voting together. */
  def forest(balanced: Boolean): RandomForestClassifier = {
    val f = new RandomForestClassifier().setLabelCol("schemeLabel").setFeaturesCol("features")
      .setNumTrees(200).setMaxDepth(10).setSeed(42)
    if (balanced) f.setWeightCol("weight") else f
  }

  /** Logistic regression for several classes, as a baseline. */
  def logistic: LogisticRegression =
    new LogisticRegression().setLabelCol("schemeLabel").setFeaturesCol("features")
      .setFamily("multinomial").setMaxIter(200).setRegParam(0.01)

  /** Adds predictedScheme, and confidence: the model's probability for the scheme it chose. */
  def predict(model: PipelineModel, df: DataFrame): DataFrame =
    model.transform(df)
      .withColumn("predictedScheme", element_at(array(Schemes.map(s => lit(s)): _*), (col("prediction") + 1).cast("int")))
      .withColumn("confidence", array_max(vector_to_array(col("probability"))))
      .drop("features", "rawPrediction", "probability", "prediction", "formatIndex", "formatVec")

  /** How many transactions of each true scheme were predicted as each scheme. */
  def confusion(predicted: DataFrame): Map[(String, String), Long] =
    predicted.groupBy("scheme", "predictedScheme").count().collect()
      .map(row => (row.getString(0), row.getString(1)) -> row.getLong(2)).toMap

  /** How much each input helped a random forest, largest first. */
  def importances(model: PipelineModel, numeric: Seq[String]): Seq[(String, Double)] = {
    val formats = model.stages(0).asInstanceOf[StringIndexerModel].labelsArray(0).toSeq.map(f => s"format=$f")
    val values  = model.stages.last.asInstanceOf[RandomForestClassificationModel].featureImportances.toArray.toSeq
    (numeric ++ formats).zip(values).sortBy { case (_, v) => -v }
  }
}

/** One scheme's results: how many transactions really had it, and precision, recall and F1. */
final case class SchemeScore(scheme: String, support: Long, precision: Double, recall: Double, f1: Double)

/** Results for every scheme, plus accuracy (the share predicted right) and macro F1 (the average F1 of the
  * schemes present, so that rare schemes count as much as common ones).
  */
final case class SchemeMetrics(perScheme: Seq[SchemeScore], accuracy: Double, macroF1: Double)

object SchemeMetrics {

  /** The metrics from a confusion table of (true scheme, predicted scheme) -> count. Plain Scala, no Spark. */
  def of(confusion: Map[(String, String), Long], schemes: Seq[String] = Stage2.Schemes): SchemeMetrics = {
    val total   = confusion.values.sum
    val correct = schemes.map(s => confusion.getOrElse((s, s), 0L)).sum
    val scores = schemes.map { s =>
      val hits      = confusion.getOrElse((s, s), 0L)
      val actual    = confusion.collect { case ((truth, _), n) if truth == s => n }.sum
      val predicted = confusion.collect { case ((_, guess), n) if guess == s => n }.sum
      val precision = if (predicted == 0) 0.0 else hits.toDouble / predicted
      val recall    = if (actual == 0) 0.0 else hits.toDouble / actual
      val f1        = if (precision + recall == 0) 0.0 else 2 * precision * recall / (precision + recall)
      SchemeScore(s, actual, precision, recall, f1)
    }
    val present = scores.filter(_.support > 0)
    SchemeMetrics(
      scores,
      if (total == 0) 0.0 else correct.toDouble / total,
      if (present.isEmpty) 0.0 else present.map(_.f1).sum / present.size)
  }
}
