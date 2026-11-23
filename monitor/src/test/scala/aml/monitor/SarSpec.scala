package aml.monitor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SarSpec extends AnyFlatSpec with Matchers {

  private def alert(id: Long, to: String, amount: Double, format: String, real: Boolean, reasons: Seq[(String, Double)]) =
    Decision(LivePayment(id, id, s"2022-09-07 00:0$id", "001|A1", to, format, isLaundering = real, scheme = None,
      features = Map("amountPaidUsd" -> amount)), score = 0.99, isAlert = true, reasons = reasons, nanos = 0)

  private val c = CaseKeeper.LiveCase(1, "FAN-OUT", "one account paid 3 others", Seq(
    alert(1, "002|B1", 1000, "ACH", real = true, Seq("New payee" -> 1.7)),
    alert(2, "003|C1", 2500, "ACH", real = false, Seq("New payee" -> 1.7, "Paid by ACH" -> 7.4)),
    alert(3, "002|B2", 500, "Cheque", real = true, Nil)))

  "Sar.of" should "collect the accounts, banks, total, dates, formats and reasons of a case" in {
    val s = Sar.of(c)
    s.accounts shouldBe Seq("001|A1", "002|B1", "002|B2", "003|C1")
    s.banks shouldBe Seq("001", "002", "003")
    (s.payments, s.totalUsd, s.real) shouldBe ((3, 4000.0, 2))
    (s.first, s.last) shouldBe (("2022-09-07 00:01", "2022-09-07 00:03"))
    s.formats shouldBe Seq("ACH" -> 2, "Cheque" -> 1)
    s.reasons shouldBe Seq("New payee" -> 2, "Paid by ACH" -> 1)
  }

  it should "write a summary in plain words, without the replay's labels" in {
    Sar.of(c).narrative shouldBe
      "Between 2022-09-07 00:01 and 2022-09-07 00:03, 4 accounts at 3 banks moved $4,000 in 3 payments (2 by ACH, 1 by Cheque). " +
        "Their pattern looks like a fan-out: one account paid 3 others. The alerts were raised because: New payee (2 of 3); Paid by ACH (1 of 3)."
  }
}
