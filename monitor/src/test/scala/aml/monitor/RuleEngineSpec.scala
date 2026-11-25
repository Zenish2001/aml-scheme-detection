package aml.monitor

import aml.common.RuleParser
import akka.Done
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.flatspec.AnyFlatSpecLike

class RuleEngineSpec extends ScalaTestWithActorTestKit with AnyFlatSpecLike {

  private val rules = RuleParser.parse(
    """rule "Over $5,000": amountPaidUsd > 5000
      |rule "Paid by ACH": format == "ACH"
      |""".stripMargin).fold(e => fail(e), identity)

  private def decision(amount: Double, format: String, real: Boolean, modelAlert: Boolean) = Decision(
    LivePayment(1, 0, "", "A", "B", format, isLaundering = real, scheme = None, features = Map("amountPaidUsd" -> amount, "format" -> format)),
    score = 0.5, isAlert = modelAlert, reasons = Nil, nanos = 0)

  "RuleEngine" should "apply every rule to every payment, beside the model, and count what each flags" in {
    val engine = spawn(RuleEngine(rules))
    val done   = createTestProbe[Done]()
    val stats  = createTestProbe[RuleEngine.Stats]()

    engine ! RuleEngine.CheckBatch(Seq(
      decision(7000, "ACH", real = true, modelAlert = true),
      decision(100, "ACH", real = false, modelAlert = false),
      decision(9000, "Cheque", real = true, modelAlert = false)), done.ref)
    done.expectMessage(Done)
    engine ! RuleEngine.Report(stats.ref)

    stats.expectMessage(RuleEngine.Stats(payments = 3, laundering = 2, modelAlerts = 1, modelReal = 1, rules = Seq(
      RuleEngine.RuleStat("Over $5,000", alerts = 2, real = 2, alsoModel = 1),
      RuleEngine.RuleStat("Paid by ACH", alerts = 2, real = 1, alsoModel = 1))))
  }

  it should "start again for a new run, ignoring batches still arriving from the old one" in {
    val engine = spawn(RuleEngine(rules))
    val done   = createTestProbe[Done]()
    val stats  = createTestProbe[RuleEngine.Stats]()

    engine ! RuleEngine.CheckBatch(Seq(decision(7000, "ACH", real = true, modelAlert = true)), done.ref)
    engine ! RuleEngine.Reset(run = 1)
    engine ! RuleEngine.CheckBatch(Seq(decision(9000, "ACH", real = true, modelAlert = true)), done.ref)           // the old run's
    engine ! RuleEngine.CheckBatch(Seq(decision(100, "Cheque", real = false, modelAlert = false)), done.ref, run = 1)
    done.receiveMessages(3)
    engine ! RuleEngine.Report(stats.ref)

    val s = stats.receiveMessage()
    (s.payments, s.laundering, s.modelAlerts) shouldBe ((1, 0, 0))
  }
}
