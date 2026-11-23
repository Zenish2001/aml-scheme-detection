package aml.batch

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AnalystHoursSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("AnalystHoursSpec")

  override def afterAll(): Unit = spark.stop()

  "AnalystHours" should "count how many top-scored payments it takes to catch a number of laundering ones" in {
    val session = spark
    import session.implicits._
    val scored = Seq((0.9, 1), (0.8, 0), (0.7, 1), (0.6, 1), (0.1, 0)).toDF("score", "isLaundering")

    AnalystHours.alertsToCatch(scored, 2) shouldBe 3   // 0.9 and 0.7 are laundering
    AnalystHours.alertsToCatch(scored, 3) shouldBe 4
    AnalystHours.alertsToCatch(scored, 0) shouldBe 0
    AnalystHours.alertsToCatch(scored, 5) shouldBe 5   // there are only 3: it takes every payment
    AnalystHours.hours(3) shouldBe 1.0                 // 3 alerts at 20 minutes each
  }
}
