package aml.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import Op._

class RuleParserSpec extends AnyFlatSpec with Matchers {

  private def condition(text: String): Condition = RuleParser.parseCondition(text).fold(e => fail(e), identity)

  private def gt(feature: String) = Compare(feature, Gt, Num(0))

  behavior of "RuleParser"

  it should "read a rule's name and condition" in {
    RuleParser.parse("""rule "New payee, over $5,000": pairPayments7d == 0 and amountPaidUsd > 5000""") shouldBe
      Right(List(Rule("New payee, over $5,000",
        And(Compare("pairPayments7d", Eq, Num(0)), Compare("amountPaidUsd", Gt, Num(5000))))))
  }

  it should "let and bind tighter than or, unless brackets say otherwise" in {
    condition("a > 0 or b > 0 and c > 0") shouldBe Or(gt("a"), And(gt("b"), gt("c")))
    condition("(a > 0 or b > 0) and c > 0") shouldBe And(Or(gt("a"), gt("b")), gt("c"))
  }

  it should "read not, every comparison, text in quotes, decimals and negative numbers" in {
    condition("""not format == "ACH" and x >= -1.5 and y != 2 and z < 0.25 and w <= 3""") shouldBe
      And(And(And(And(
        Not(Compare("format", Eq, Text("ACH"))),
        Compare("x", Ge, Num(-1.5))),
        Compare("y", Ne, Num(2))),
        Compare("z", Lt, Num(0.25))),
        Compare("w", Le, Num(3)))
  }

  it should "read several rules, skipping comments and blank lines" in {
    val text =
      """# Two rules
        |rule "first": a > 1
        |
        |# the second, over two lines
        |rule "second":
        |  b == 0 or c == 0
        |""".stripMargin
    RuleParser.parse(text).map(_.map(_.name)) shouldBe Right(List("first", "second"))
  }

  it should "point to the line and column of a mistake" in {
    val result = RuleParser.parse("rule \"ok\": a > 1\nrule \"bad\": amountPaidUsd 5000")
    result.isLeft shouldBe true
    result.left.getOrElse("") should (include("line 2") and include("expected a comparison"))
  }

  it should "refuse a keyword as a feature name, and text without its closing quote" in {
    RuleParser.parseCondition("and > 0").isLeft shouldBe true
    RuleParser.parseCondition("format == \"ACH").isLeft shouldBe true
  }

  it should "turn a condition back into text that reads back the same" in {
    val original = condition("""not (a > 0 or b == "x") and c <= -2.5 or d != 0.001""")
    condition(Rules.show(original)) shouldBe original
  }
}
