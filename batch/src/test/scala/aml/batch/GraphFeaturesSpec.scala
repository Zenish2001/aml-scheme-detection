package aml.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GraphFeaturesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("GraphFeaturesSpec")

  override def afterAll(): Unit = spark.stop()

  /** Tiny transactions on 1 September 2022: (time, sender, receiver, payment format). */
  private def txns(rows: (String, String, String, String)*): DataFrame = {
    val session = spark
    import session.implicits._
    rows.toDF("time", "senderAccount", "receiverAccount", "format")
      .withColumn("ts", to_timestamp(concat(lit("2022-09-01 "), col("time")), "yyyy-MM-dd HH:mm"))
      .withColumn("hour", Features.hourOf(col("ts")))
      .withColumn("txnId", monotonically_increasing_id())
  }

  private def graph(input: DataFrame): DataFrame = GraphFeatures.withGraphFeatures(input, GraphFeatures.hubs(input))

  /** One feature, looked up by each transaction's time and sender. */
  private def feature(df: DataFrame, name: String): Map[(String, String), Long] =
    df.select(col("time"), col("senderAccount"), col(name).cast("long")).collect()
      .map(row => (row.getString(0), row.getString(1)) -> row.getLong(2)).toMap

  behavior of "GraphFeatures"

  it should "find payments back and 3-step loops, but only in time order" in {
    val result = graph(txns(
      ("09:00", "R", "S", "ACH"),
      ("10:00", "R", "X", "ACH"),
      ("11:00", "X", "S", "ACH"),
      ("12:00", "S", "R", "ACH"),     // closes R -> X -> S -> R, and R paid S at 09:00
      ("10:00", "X2", "S2", "ACH"),
      ("11:00", "R2", "X2", "ACH"),   // wrong order: X2 paid S2 before R2 paid X2
      ("12:00", "S2", "R2", "ACH")))

    feature(result, "cyclePaths7d")(("12:00", "S")) shouldBe 1L
    feature(result, "reciprocal7d")(("12:00", "S")) shouldBe 1L
    feature(result, "cyclePaths7d")(("12:00", "S2")) shouldBe 0L
  }

  it should "count the other go-betweens from the same source to the same destination" in {
    val result = graph(txns(
      ("10:00", "A", "M1", "ACH"), ("10:00", "A", "M2", "ACH"), ("10:00", "A", "M3", "ACH"),
      ("11:00", "M2", "B", "ACH"),
      ("12:00", "M3", "B", "ACH"),    // M2 had already carried A's money to B
      ("13:00", "M1", "B", "ACH")))   // M2 and M3 had

    val scatter = feature(result, "scatterGatherPaths7d")
    scatter(("11:00", "M2")) shouldBe 0L
    scatter(("12:00", "M3")) shouldBe 1L
    scatter(("13:00", "M1")) shouldBe 2L
  }

  it should "skip hub accounts as go-betweens" in {
    val hubPayments = (1 to 101).map(i => ("09:00", "H", s"C$i", "ACH"))   // H has 103 counterparties in all
    val input  = txns(hubPayments ++ Seq(("10:00", "R", "H", "ACH"), ("11:00", "H", "S", "ACH"), ("12:00", "S", "R", "ACH")): _*)
    val result = graph(input)

    GraphFeatures.hubs(input).collect().map(_.getString(0)) shouldBe Array("H")
    feature(result, "cyclePaths7d")(("12:00", "S")) shouldBe 0L   // R -> H -> S runs through a hub
  }

  it should "count receipts through the other rail, and flag Bitcoin payments" in {
    val result = graph(txns(
      ("10:00", "W", "S", "Bitcoin"),
      ("12:00", "S", "T", "ACH"),        // received Bitcoin, now pays by bank
      ("13:00", "S", "U", "Bitcoin")))   // received no bank payments

    feature(result, "senderRecvOtherRail24h")(("12:00", "S")) shouldBe 1L
    feature(result, "senderRecvOtherRail24h")(("13:00", "S")) shouldBe 0L
    feature(result, "isBitcoin") shouldBe Map(("10:00", "W") -> 1L, ("12:00", "S") -> 0L, ("13:00", "S") -> 1L)
  }

  it should "keep every transaction, with no empty features" in {
    val input  = txns(("10:00", "A", "B", "ACH"), ("11:00", "B", "C", "Wire"), ("12:00", "C", "A", "Bitcoin"))
    val result = graph(input)

    result.count() shouldBe 3
    result.filter(GraphFeatures.columns.map(c => col(c).isNull).reduce(_ || _)).count() shouldBe 0
  }
}
