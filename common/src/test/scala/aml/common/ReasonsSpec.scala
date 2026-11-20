package aml.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReasonsSpec extends AnyFlatSpec with Matchers {

  private val row: Map[String, Any] = Map(
    "senderMinutesSinceReceived" -> 12L, "amountVsSenderAverage24h" -> 4.2345,
    "amountPaidUsd" -> 12500.37, "pairPayments7d" -> 0L)

  private def reasons(text: String): List[Rule] = RuleParser.parse(text).fold(e => fail(e), identity)

  behavior of "Reasons"

  it should "fill each {feature} with the transaction's value, written as a person would" in {
    Reasons.render("Money passed on {senderMinutesSinceReceived} minutes after it arrived", row) shouldBe
      "Money passed on 12 minutes after it arrived"
    Reasons.render("{amountVsSenderAverage24h} times the sender's average payment", row) shouldBe
      "4.2 times the sender's average payment"
    Reasons.render("Large payment: ${amountPaidUsd}", row) shouldBe "Large payment: $12,500"
  }

  it should "list the features a reason's text refers to" in {
    Reasons.placeholders("{a} and {b}, but not b") shouldBe List("a", "b")
    Reasons.placeholders("No values here") shouldBe Nil
  }

  it should "give every reason a transaction matches, in the order given, and no others" in {
    val given = reasons(
      """rule "Money passed on {senderMinutesSinceReceived} minutes after it arrived": senderMinutesSinceReceived < 60
        |rule "Never matches": pairPayments7d > 5
        |rule "New payee": pairPayments7d == 0
        |""".stripMargin)
    Reasons.explain(given, row) shouldBe List("Money passed on 12 minutes after it arrived", "New payee")
  }

  it should "say which feature a reason's text needs but doesn't have" in {
    the[NoSuchElementException] thrownBy Reasons.render("{unknownFeature} times", row) should
      have message "no value for feature unknownFeature"
  }
}
