package aml.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Stage2Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("Stage2Spec")

  override def afterAll(): Unit = spark.stop()

  behavior of "SchemeMetrics"

  it should "compute precision, recall, F1, accuracy and macro F1 from a confusion table" in {
    val confusion = Map(("A", "A") -> 8L, ("A", "B") -> 2L, ("B", "A") -> 1L, ("B", "B") -> 9L)
    val m = SchemeMetrics.of(confusion, Seq("A", "B", "C"))
    val a = m.perScheme.find(_.scheme == "A").get
    val b = m.perScheme.find(_.scheme == "B").get

    m.accuracy shouldBe 0.85 +- 1e-9                          // 17 of 20 right
    a.precision shouldBe 8.0 / 9 +- 1e-9                      // 9 predicted A, 8 of them really A
    a.recall shouldBe 0.8 +- 1e-9                             // 10 really A, 8 found
    b.f1 shouldBe 2 * (9.0 / 11) * 0.9 / (9.0 / 11 + 0.9) +- 1e-9
    m.macroF1 shouldBe (a.f1 + b.f1) / 2 +- 1e-9              // C has no transactions, so it's left out
    m.perScheme.find(_.scheme == "C").get shouldBe SchemeScore("C", 0, 0.0, 0.0, 0.0)
  }

  behavior of "Stage2"

  it should "keep only listed laundering, and give each scheme its own label" in {
    val session = spark
    import session.implicits._
    val df = (Stage2.Schemes.map(s => (1, s)) ++ Seq((1, "UNASSIGNED"), (0, null))).toDF("isLaundering", "scheme")
    val labelled = Stage2.withSchemeLabel(Stage2.listed(df))

    labelled.count() shouldBe 8
    labelled.select("scheme", "schemeLabel").collect().map(r => r.getString(0) -> r.getDouble(1)).toMap shouldBe
      Stage2.Schemes.zipWithIndex.map { case (s, i) => s -> i.toDouble }.toMap
  }

  it should "weight rare schemes up and common ones down, averaging 1" in {
    val session = spark
    import session.implicits._
    val train   = (Seq.fill(6)("FAN-OUT") ++ Seq.fill(2)("CYCLE")).toDF("scheme")
    val weights = Stage2.withClassWeights(train).select("scheme", "weight").distinct().collect()
      .map(r => r.getString(0) -> r.getDouble(1)).toMap

    weights("FAN-OUT") shouldBe 8.0 / 12 +- 1e-9   // 8 rows, 2 schemes, 6 FAN-OUT
    weights("CYCLE") shouldBe 2.0 +- 1e-9
  }

  it should "train a model that names the right scheme when one feature gives it away" in {
    val numeric = Stage1.numericFeatures(includeHour = false)
    val which   = col("id") % 3
    val data = spark.range(150).select(
      numeric.map(n => (if (n == "cyclePaths7d") which.cast("double") else lit(0.0)).as(n)) ++
        Seq(lit("ACH").as("format"), lit(1).as("isLaundering"),
          element_at(array(lit("FAN-OUT"), lit("FAN-IN"), lit("CYCLE")), (which + 1).cast("int")).as("scheme")): _*)
    val train = Stage2.withSchemeLabel(data)

    val model     = Stage1.pipeline(numeric, Stage2.forest(balanced = false)).fit(train)
    val predicted = Stage2.predict(model, train)
    predicted.filter(col("predictedScheme") =!= col("scheme")).count() shouldBe 0
  }
}
