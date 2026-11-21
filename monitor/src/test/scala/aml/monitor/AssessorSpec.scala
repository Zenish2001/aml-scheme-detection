package aml.monitor

import aml.common.{Leaf, RuleParser, ScoringModel, SplitNode, WeightedTree}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AssessorSpec extends AnyFlatSpec with Matchers {

  // One tree: over $5,000 scores high, otherwise low
  private val model = ScoringModel(Seq("amountPaidUsd", "pairPayments7d"), Seq("ACH"),
    Seq(WeightedTree(1.0, SplitNode(0, 5000.0, Leaf(-2.0), Leaf(2.0)))))

  private val reasons = RuleParser.parse(
    """rule "New payee": pairPayments7d == 0
      |rule "Large payment: ${amountPaidUsd}": amountPaidUsd >= 10000
      |rule "Not kept": amountPaidUsd > 0
      |""".stripMargin).fold(e => fail(e), identity)

  private val settings = MonitorSettings.parse(
    """# test settings
      |threshold 0.9
      |reason 1.7 New payee
      |reason 3.0 Large payment: ${amountPaidUsd}
      |""".stripMargin).fold(e => fail(e), identity)

  private def payment(amount: Double, pair: Double) = LivePayment(1, 0, "2022-09-07 00:00", "A", "B", "ACH", isLaundering = false,
    scheme = None, features = Map("amountPaidUsd" -> amount, "pairPayments7d" -> pair, "format" -> "ACH"))

  "MonitorSettings" should "read the threshold and each reason's lift" in {
    settings.threshold shouldBe 0.9
    settings.reasonLifts shouldBe Map("New payee" -> 1.7, "Large payment: ${amountPaidUsd}" -> 3.0)
    MonitorSettings.parse("reason 1.0 x").isLeft shouldBe true
  }

  "Assessor" should "alert above the threshold, with the kept reasons, strongest first" in {
    val d = Assessor(model, settings, reasons).assess(payment(12000.0, 0.0))
    d.isAlert shouldBe true
    d.score shouldBe (1.0 / (1.0 + math.exp(-4.0)) +- 1e-12)
    d.reasons shouldBe Seq("Large payment: $12,000" -> 3.0, "New payee" -> 1.7)
  }

  it should "not alert, or explain, below the threshold" in {
    val d = Assessor(model, settings, reasons).assess(payment(100.0, 0.0))
    d.isAlert shouldBe false
    d.reasons shouldBe empty
  }

  "AlertJson" should "write an alert as one line of JSON, with quotes escaped" in {
    val d = Decision(payment(12000.0, 0.0).copy(sender = "A\"1"), 0.95, isAlert = true, Seq("New payee" -> 1.7), 1000)
    AlertJson.of(d) shouldBe
      """{"txnId":1,"time":"2022-09-07 00:00","sender":"A\"1","receiver":"B","amountUsd":12000.0,"format":"ACH","score":0.95,""" +
        """"reasons":[{"text":"New payee","lift":1.7}],"isLaundering":false,"scheme":null}"""
  }
}
