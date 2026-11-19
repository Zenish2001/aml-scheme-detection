package aml.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RulesSpec extends AnyFlatSpec with Matchers {

  private def condition(text: String): Condition = RuleParser.parseCondition(text).fold(e => fail(e), identity)

  // One transaction, with values of different types, as the live monitor will hold them
  private val row: Map[String, Any] =
    Map("pairPayments7d" -> 0L, "amountPaidUsd" -> 7500.0, "format" -> "ACH", "isSelfTransfer" -> 0)

  behavior of "Rules.matches"

  it should "compare numbers of any type, and text" in {
    Rules.matches(condition("pairPayments7d == 0"), row) shouldBe true
    Rules.matches(condition("amountPaidUsd > 5000"), row) shouldBe true
    Rules.matches(condition("amountPaidUsd <= 5000"), row) shouldBe false
    Rules.matches(condition("isSelfTransfer != 1"), row) shouldBe true
    Rules.matches(condition("format == \"ACH\""), row) shouldBe true
    Rules.matches(condition("format != \"ACH\""), row) shouldBe false
  }

  it should "combine conditions with and, or and not" in {
    Rules.matches(condition("pairPayments7d == 0 and amountPaidUsd > 9000"), row) shouldBe false
    Rules.matches(condition("pairPayments7d == 0 and amountPaidUsd > 9000 or format == \"ACH\""), row) shouldBe true
    Rules.matches(condition("not (pairPayments7d == 0)"), row) shouldBe false
  }

  it should "say which feature is missing" in {
    the[NoSuchElementException] thrownBy Rules.matches(condition("unknownFeature > 0"), row) should
      have message "no value for feature unknownFeature"
  }

  behavior of "Rules.features"

  it should "list every feature a condition uses" in {
    Rules.features(condition("a > 0 and (b == 1 or not c < 2)")) shouldBe Set("a", "b", "c")
  }
}
