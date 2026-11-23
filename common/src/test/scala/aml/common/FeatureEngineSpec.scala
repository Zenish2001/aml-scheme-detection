package aml.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FeatureEngineSpec extends AnyFlatSpec with Matchers {

  private val usd = Map("US Dollar" -> 1.0, "Euro" -> 1.2)

  /** A payment between accounts written "bank|account". */
  private def pay(id: Long, minute: Long, from: String, to: String, amount: Double = 100, format: String = "ACH",
                  payCcy: String = "US Dollar", recvCcy: String = "US Dollar") = {
    val Array(fb, fa) = from.split('|'); val Array(tb, ta) = to.split('|')
    RawPayment(id, minute, fb, fa, tb, ta, amount, recvCcy, amount, payCcy, format)
  }

  private def run(ps: Seq[RawPayment], hubs: Set[String] = Set.empty): Map[Long, Map[String, Double]] =
    FeatureEngine.run(ps.iterator, usd, hubs).map { case (p, f) => p.id -> f }.toMap

  "FeatureEngine" should "look only at the 24 and 168 whole hours before a payment's hour" in {
    val f = run(Seq(
      pay(1, 0, "1|A", "1|B", 100),          // hour 0
      pay(2, 70, "1|A", "2|C", 50),          // hour 1
      pay(3, 130, "1|A", "1|B", 30),         // hour 2
      pay(4, 135, "1|A", "1|B", 10),         // hour 2: doesn't see payment 3
      pay(5, 170 * 60, "1|A", "1|B", 20)))   // hour 170: a week after hour 2

    f(3)("senderSent24hCount") shouldBe 2
    f(3)("senderSent24hUsd") shouldBe 150
    f(3)("senderSent24hMaxUsd") shouldBe 100
    f(3)("senderSent24hParties") shouldBe 2
    f(3)("receiverRecv24hCount") shouldBe 1
    f(3)("pairPayments7d") shouldBe 1
    f(3)("amountVsSenderAverage24h") shouldBe 0.4
    f(3)("isSameBank") shouldBe 1
    f(4)("senderSent24hCount") shouldBe 2
    f(5)("senderSent24hCount") shouldBe 0
    f(5)("pairPayments7d") shouldBe 2           // payments 3 and 4, at hour 2, are still inside the week
    f(5)("amountVsSenderAverage24h") shouldBe FeatureEngine.NoHistory
  }

  it should "count the minutes since the sender last received money, ignoring the same minute" in {
    val f = run(Seq(
      pay(1, 10, "1|X", "1|A"),
      pay(2, 25, "1|A", "1|Y"),
      pay(3, 30, "1|Z", "1|A"),
      pay(4, 30, "1|A", "1|Y"),
      pay(5, 40, "1|B", "1|Y")))
    f(2)("senderMinutesSinceReceived") shouldBe 15
    f(4)("senderMinutesSinceReceived") shouldBe 20   // the money from Z, in the same minute, doesn't count
    f(5)("senderMinutesSinceReceived") shouldBe FeatureEngine.MaxMinutes
  }

  it should "find loops back to the sender, in time order, but not through hubs" in {
    val loop = Seq(pay(1, 60, "1|R", "1|X"), pay(2, 120, "1|X", "1|S"), pay(3, 180, "1|R", "1|S"), pay(4, 240, "1|S", "1|R"))
    run(loop)(4)("cyclePaths7d") shouldBe 1
    run(loop)(4)("reciprocal7d") shouldBe 1          // R paid S at hour 3
    run(loop, hubs = Set("1|X"))(4)("cyclePaths7d") shouldBe 0

    val wrongOrder = Seq(pay(1, 60, "1|X", "1|S"), pay(2, 120, "1|R", "1|X"), pay(3, 180, "1|S", "1|R"))
    run(wrongOrder)(3)("cyclePaths7d") shouldBe 0
  }

  it should "find other go-betweens carrying money from the same source to the same receiver" in {
    val scatter = Seq(pay(1, 60, "1|P", "1|S"), pay(2, 60, "1|P", "1|X"), pay(3, 120, "1|X", "1|R"), pay(4, 180, "1|S", "1|R"))
    run(scatter)(4)("scatterGatherPaths7d") shouldBe 1
    run(scatter, hubs = Set("1|S"))(4)("scatterGatherPaths7d") shouldBe 0
  }

  it should "convert to US dollars, flag facts about the payment, and count receipts through the other rail" in {
    val f = run(Seq(
      pay(1, 0, "1|C", "1|A", 1, format = "Bitcoin"),
      pay(2, 60, "1|A", "2|D", 8000, payCcy = "Euro", recvCcy = "US Dollar")))
    f(2)("amountPaidUsd") shouldBe (9600.0 +- 1e-9)
    f(2)("isJustUnder10k") shouldBe 1
    f(2)("isCrossCurrency") shouldBe 1
    f(2)("senderRecvOtherRail24h") shouldBe 1
    f(1)("isBitcoin") shouldBe 1
  }

  "FeatureEngine.hubs" should "find accounts with more than 100 different counterparties" in {
    val many = (1 to 101).map(i => pay(i, i, "1|H", s"1|P$i"))
    FeatureEngine.hubs(many.iterator) shouldBe Set("1|H")
    FeatureEngine.hubs(many.take(100).iterator) shouldBe empty
  }
}
