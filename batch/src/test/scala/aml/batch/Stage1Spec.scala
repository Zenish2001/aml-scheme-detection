package aml.batch

import java.sql.Date
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Stage1Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("Stage1Spec")

  override def afterAll(): Unit = spark.stop()

  behavior of "Stage1"

  it should "never give the model the label, the date or an ID" in {
    Stage1.numericFeatures(includeHour = true) should have length 35
    Stage1.numericFeatures(includeHour = true) should contain noneOf ("isLaundering", "label", "scheme", "day", "hour", "ts", "timestamp", "txnId")
    Stage1.numericFeatures(includeHour = false) should not contain "hourOfDay"
  }

  it should "only use features the feature code really produces" in {
    val session = spark
    import session.implicits._
    val raw = Seq(("2022-09-01 10:00", "A", "B", 10.0, "ACH"), ("2022-09-01 11:00", "B", "C", 10.0, "Wire"), ("2022-09-01 12:00", "C", "A", 10.0, "Cash"))
      .toDF("time", "fromAcct", "toAcct", "amountPaidUsd", "format")
      .withColumn("ts", to_timestamp(col("time"), "yyyy-MM-dd HH:mm"))
      .withColumn("fromBank", lit("001")).withColumn("toBank", lit("001"))
      .withColumn("amountReceivedUsd", col("amountPaidUsd"))
      .withColumn("payCcy", lit("US Dollar")).withColumn("recvCcy", lit("US Dollar"))
      .withColumn("txnId", monotonically_increasing_id())
    val account  = Features.withAccountFeatures(raw, Features.last24Hours(Features.hourlyActivity(raw)))
    val featured = TransactionFeatures.addAll(account)
    val all      = GraphFeatures.withGraphFeatures(featured, GraphFeatures.hubs(featured))

    all.columns.toSeq should contain allElementsOf Stage1.numericFeatures(includeHour = true)
  }

  it should "raise each day's highest-scored transactions as that day's alerts" in {
    val session = spark
    import session.implicits._
    val day1 = Date.valueOf("2022-09-07")
    val day2 = Date.valueOf("2022-09-08")
    val scored = Seq((day1, 0.9, 1), (day1, 0.5, 0), (day1, 0.1, 1), (day2, 0.8, 0), (day2, 0.7, 1))
      .toDF("day", "score", "isLaundering")

    val alerts = Stage1.atDailyBudget(scored, perDay = 1)   // 0.9 on day 1 and 0.8 on day 2
    alerts shouldBe Stage1.Alerts(alerts = 2, caught = 1, laundering = 3)
    alerts.recall shouldBe (1.0 / 3 +- 1e-9)
    alerts.precision shouldBe 0.5
  }

  it should "train a model that ranks laundering first when one feature gives it away" in {
    val numeric = Stage1.numericFeatures(includeHour = true)
    val isTen   = col("id") % 10 === 0
    val data = spark.range(300).select(
      numeric.map(n => (if (n == "cyclePaths7d") isTen.cast("double") else lit(0.0)).as(n)) ++
        Seq(lit("ACH").as("format"), isTen.cast("int").as("isLaundering")): _*)

    val model = Stage1.pipeline(numeric, Stage1.gbt(iterations = 5)).fit(Stage1.withLabel(data))
    Stage1.prAuc(Stage1.score(model, data)) should be > 0.99
  }
}
