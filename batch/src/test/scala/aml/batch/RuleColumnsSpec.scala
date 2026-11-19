package aml.batch

import aml.common.{RuleParser, Rules}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RuleColumnsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("RuleColumnsSpec")

  override def afterAll(): Unit = spark.stop()

  "RuleColumns.toColumn" should "match exactly the rows that the plain-Scala evaluator matches" in {
    val session = spark
    import session.implicits._
    val txns = Seq(
      (1, 0L, 7500.0, "ACH", 0), (2, 3L, 7500.0, "Wire", 0), (3, 0L, 120.0, "ACH", 1),
      (4, 0L, 9500.0, "Cash", 0), (5, 1L, 0.0, "ACH", 0))
      .toDF("id", "pairPayments7d", "amountPaidUsd", "format", "isSelfTransfer")
    val rows = txns.collect().map(row => row.getValuesMap[Any](row.schema.fieldNames.toSeq))

    val conditions = Seq(
      "pairPayments7d == 0 and amountPaidUsd > 5000",
      "format == \"ACH\" or isSelfTransfer == 1",
      "not (pairPayments7d == 0) and format != \"Cash\"",
      "amountPaidUsd >= 9000 or amountPaidUsd < 100 and pairPayments7d <= 0")

    conditions.foreach { text =>
      val condition = RuleParser.parseCondition(text).fold(e => fail(e), identity)
      val bySpark   = txns.filter(RuleColumns.toColumn(condition)).select("id").collect().map(_.getInt(0)).toSet
      val byScala   = rows.filter(row => Rules.matches(condition, row)).map(_("id").asInstanceOf[Int]).toSet
      withClue(text) { bySpark shouldBe byScala }
    }

    val first = RuleParser.parseCondition(conditions.head).fold(e => fail(e), identity)
    txns.filter(RuleColumns.toColumn(first)).select("id").collect().map(_.getInt(0)).toSet shouldBe Set(1, 4)
  }
}
