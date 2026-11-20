package aml.batch

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EllipticModelSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("EllipticModelSpec")

  override def afterAll(): Unit = spark.stop()

  behavior of "EllipticModel"

  it should "use 93 local and 72 aggregated features, never the time step, the ID or the label" in {
    EllipticModel.local should have length 93
    EllipticModel.all should have length 165
    EllipticModel.all should contain noneOf ("timeStep", "txId", "label", "isIllicit")
  }

  it should "keep only labelled transactions" in {
    val session = spark
    import session.implicits._
    val df = Seq((1L, Some(1)), (2L, Some(0)), (3L, None)).toDF("txId", "isIllicit")
    EllipticModel.labelled(df).select("txId").collect().map(_.getLong(0)).toSet shouldBe Set(1L, 2L)
  }

  it should "count flags at a threshold, with precision, recall and F1" in {
    val session = spark
    import session.implicits._
    val scored = Seq((0.9, 1.0), (0.6, 0.0), (0.4, 1.0), (0.2, 0.0)).toDF("score", "label")
    val flags  = EllipticModel.atThreshold(scored)

    flags shouldBe EllipticModel.Flags(flagged = 2, caught = 1, illicit = 2)
    flags.precision shouldBe 0.5
    flags.recall shouldBe 0.5
    flags.f1 shouldBe 0.5
  }

  it should "flag each time step's highest scores within a budget, and count each step apart" in {
    val session = spark
    import session.implicits._
    val scored = Seq((35, 0.9, 1.0), (35, 0.8, 0.0), (36, 0.7, 0.0), (36, 0.3, 1.0)).toDF("timeStep", "score", "label")

    EllipticModel.atStepBudget(scored, perStep = 1) shouldBe EllipticModel.Flags(flagged = 2, caught = 1, illicit = 2)
    EllipticModel.byStep(scored) shouldBe Seq(
      35 -> EllipticModel.Flags(flagged = 2, caught = 1, illicit = 1),
      36 -> EllipticModel.Flags(flagged = 1, caught = 0, illicit = 1))
  }
}
