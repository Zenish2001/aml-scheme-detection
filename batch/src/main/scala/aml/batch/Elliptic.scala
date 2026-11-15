package aml.batch

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Reads Elliptic's real Bitcoin dataset (Weber et al., 2019): transactions with anonymized features,
  * the payments between them, and labels for some.
  *
  *  - elliptic_txs_features.csv has no header: txId, a time step from 1 to 49, then 165 features,
  *    93 local ones (about the transaction itself) and 72 aggregated over its neighbors.
  *  - elliptic_txs_classes.csv has a header (txId, class): 1 = illicit, 2 = licit, otherwise unknown.
  *  - elliptic_txs_edgelist.csv has a header (txId1, txId2): money flowed from txId1 to txId2.
  */
object Elliptic {

  val Illicit = "illicit"
  val Licit   = "licit"
  val Unknown = "unknown"

  val LocalFeatures      = 93   // the time step counts as the 94th local feature in the paper
  val AggregatedFeatures = 72

  val featureNames: Seq[String] =
    (1 to LocalFeatures).map(i => s"local_$i") ++ (1 to AggregatedFeatures).map(i => s"agg_$i")

  val featureSchema: StructType = StructType(
    StructField("txId", LongType) +: StructField("timeStep", IntegerType) +: featureNames.map(StructField(_, DoubleType)))

  /** One row per transaction: txId, timeStep, then the 165 features, all with our own names. */
  def readFeatures(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "false").option("mode", "PERMISSIVE").schema(featureSchema).csv(path)

  /** True for a row where every column parsed. */
  val isValid: Column = featureSchema.fieldNames.toSeq.map(name => col(name).isNotNull).reduce(_ && _)

  /** One row per labelled transaction: txId, label (illicit, licit or unknown), and isIllicit (1, 0, or null when unknown). */
  def readClasses(spark: SparkSession, path: String): DataFrame = {
    val schema = StructType(Seq(StructField("txId", LongType), StructField("class", StringType)))
    spark.read.option("header", "true").schema(schema).csv(path)
      .withColumn("label", when(col("class") === "1", Illicit).when(col("class") === "2", Licit).otherwise(Unknown))
      .withColumn("isIllicit", when(col("class") === "1", 1).when(col("class") === "2", 0))
      .drop("class")
  }

  /** One row per payment between two transactions: src paid into dst. */
  def readEdges(spark: SparkSession, path: String): DataFrame = {
    val schema = StructType(Seq(StructField("txId1", LongType), StructField("txId2", LongType)))
    spark.read.option("header", "true").schema(schema).csv(path)
      .select(col("txId1").as("src"), col("txId2").as("dst"))
  }

  /** The features with each transaction's label; a transaction missing from the classes file counts as unknown. */
  def transactions(features: DataFrame, classes: DataFrame): DataFrame =
    features.join(classes, Seq("txId"), "left")
      .withColumn("label", coalesce(col("label"), lit(Unknown)))
}
