package aml.monitor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReplaySpec extends AnyFlatSpec with Matchers {

  private val header = "minute,time,txnId,senderAccount,receiverAccount,format,isLaundering,scheme,amountPaidUsd,pairPayments7d"

  "Replay.parser" should "read a payment, with every non-text column as a numeric feature" in {
    val p = Replay.parser(header)("27703291,2022-09-07 05:51,42,014|A,001|B,ACH,1,SCATTER-GATHER,14457.0,0")

    p.txnId shouldBe 42L
    p.minute shouldBe 27703291L
    p.time shouldBe "2022-09-07 05:51"
    (p.sender, p.receiver, p.format) shouldBe (("014|A", "001|B", "ACH"))
    p.isLaundering shouldBe true
    p.scheme shouldBe Some("SCATTER-GATHER")
    p.features("amountPaidUsd") shouldBe 14457.0
    p.features("pairPayments7d") shouldBe 0.0
    p.features("format") shouldBe "ACH"
  }

  it should "read a normal payment as having no scheme" in {
    Replay.parser(header)("1,2022-09-07 00:01,7,A,B,Cheque,0,NONE,12.5,3").scheme shouldBe None
  }

  it should "refuse a file without the columns it needs" in {
    the[IllegalArgumentException] thrownBy Replay.parser("minute,txnId") should have message
      "requirement failed: the replay file has no time, senderAccount, receiverAccount, format, isLaundering, scheme column"
  }
}
