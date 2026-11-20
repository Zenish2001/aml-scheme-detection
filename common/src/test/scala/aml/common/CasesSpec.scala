package aml.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CasesSpec extends AnyFlatSpec with Matchers {

  /** Payments written as "A>B" (A paid B), a minute apart. */
  private def payments(arrows: String*): Seq[Payment] = arrows.zipWithIndex.map { case (arrow, i) =>
    val Array(from, to) = arrow.split(">")
    Payment(i.toLong, from, to, i.toLong)
  }

  private def scheme(arrows: String*): String = Cases.shapeOf(payments(arrows: _*)).scheme

  behavior of "Cases.shapeOf"

  it should "recognize one account paying many, and many paying one" in {
    scheme("A>B", "A>C", "A>D") shouldBe "FAN-OUT"
    scheme("B>A", "C>A", "D>A") shouldBe "FAN-IN"
    scheme("A>B", "A>B", "A>C") shouldBe "FAN-OUT"   // repeated payments count once
  }

  it should "recognize money going round a loop" in {
    scheme("A>B", "B>C", "C>A") shouldBe "CYCLE"
  }

  it should "recognize go-betweens, and a collector that pays out" in {
    scheme("A>M1", "A>M2", "M1>Z", "M2>Z") shouldBe "SCATTER-GATHER"
    scheme("A>H", "B>H", "H>X", "H>Y") shouldBe "GATHER-SCATTER"
  }

  it should "recognize one group paying another, and layers of them" in {
    scheme("A>X", "A>Y", "B>X", "B>Y") shouldBe "BIPARTITE"
    scheme("A1>B1", "A1>B2", "A2>B1", "B1>C1", "B2>C2") shouldBe "STACK"
  }

  it should "call a chain or an irregular path random, and say why" in {
    Cases.shapeOf(payments("A>B", "B>C", "C>D")) shouldBe Shape("RANDOM", "a chain of payments through 4 accounts")
    Cases.shapeOf(payments("A>M1", "A>M2", "M1>Z", "M2>Z")).why shouldBe
      "one account paid 2 go-betweens, which all paid one account"
  }

  behavior of "Cases.group"

  it should "put payments that share an account in one case, and others apart" in {
    val cases = Cases.group(payments("A>B", "X>Y", "B>C"), windowMinutes = 60)
    cases.map(_.map(_.id)) shouldBe Seq(Seq(0L, 2L), Seq(1L))
  }

  it should "keep payments apart when they're further apart in time than the window" in {
    val far = Seq(Payment(0, "A", "B", 0), Payment(1, "A", "C", 10000))
    Cases.group(far, windowMinutes = 4320) should have size 2   // 3 days
    Cases.group(far, windowMinutes = 20000) should have size 1
  }
}
