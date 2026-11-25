package aml.monitor

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.flatspec.AnyFlatSpecLike

class CaseKeeperSpec extends ScalaTestWithActorTestKit with AnyFlatSpecLike {

  private def alert(id: Long, from: String, to: String, minute: Long) = Decision(
    LivePayment(id, minute, "", from, to, "ACH", isLaundering = true, scheme = None, features = Map.empty),
    score = 0.99, isAlert = true, reasons = Nil, nanos = 0)

  "CaseKeeper" should "group alerts that share an account into cases, and name their shapes" in {
    val keeper = spawn(CaseKeeper())
    val probe  = createTestProbe[CaseKeeper.Summary]()

    Seq(alert(1, "A", "B", 0), alert(2, "A", "C", 1), alert(3, "A", "D", 2), alert(4, "X", "Y", 3))
      .foreach(a => keeper ! CaseKeeper.Alerted(a))
    keeper ! CaseKeeper.Report(probe.ref)

    val summary = probe.receiveMessage()
    summary.alerts shouldBe 4
    summary.cases.map(c => (c.scheme, c.alerts.size)) shouldBe Seq(("FAN-OUT", 3), ("RANDOM", 1))
  }

  it should "forget every alert when told to reset, for a new file" in {
    val keeper = spawn(CaseKeeper())
    val probe  = createTestProbe[CaseKeeper.Summary]()
    keeper ! CaseKeeper.Alerted(alert(1, "A", "B", 0))
    keeper ! CaseKeeper.Reset
    keeper ! CaseKeeper.Report(probe.ref)
    probe.receiveMessage().alerts shouldBe 0
  }
}
