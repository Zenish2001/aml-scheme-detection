package aml.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Part 8a: our own graph features for Elliptic, from its payment flows. Elliptic's nodes are transactions, and a
  * Bitcoin transaction can only spend outputs of earlier ones, so its graph has no loops: the features count
  * neighbours instead. For each transaction:
  *  - inDegree, outDegree: transactions that paid into it, and that it paid into
  *  - in2, out2:           different transactions two steps back and two steps forward
  * Elliptic's flows never cross time steps, so these never mix time steps either.
  */
object EllipticGraph {

  val columns: Seq[String] = Seq("inDegree", "outDegree", "in2", "out2")

  /** One row per transaction with at least one flow: txId and the four graph features. Needs src and dst. */
  def features(edges: DataFrame): DataFrame = {
    val outDegree = edges.groupBy(col("src").as("txId")).agg(count(lit(1)).as("outDegree"))
    val inDegree  = edges.groupBy(col("dst").as("txId")).agg(count(lit(1)).as("inDegree"))
    val twoSteps  = edges.select(col("src").as("a"), col("dst").as("b"))
      .join(edges.select(col("src").as("b"), col("dst").as("c")), Seq("b"))
      .filter(col("a") =!= col("c"))
    val out2 = twoSteps.groupBy(col("a").as("txId")).agg(countDistinct("c").as("out2"))
    val in2  = twoSteps.groupBy(col("c").as("txId")).agg(countDistinct("a").as("in2"))
    Seq(inDegree, in2, out2).foldLeft(outDegree)((acc, df) => acc.join(df, Seq("txId"), "full_outer"))
  }

  /** The transactions with the four graph features; a transaction with no flows gets zeros. */
  def withGraph(txns: DataFrame, edges: DataFrame): DataFrame =
    txns.join(features(edges), Seq("txId"), "left").na.fill(0L, columns)
}
