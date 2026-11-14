package aml.batch

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PatternsSpec extends AnyFlatSpec with Matchers {

  // The start of the real HI-Small_Patterns.txt, shortened
  private val fanOut1 = "2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1"
  private val fanOut2 = "2022/09/01 04:33,021174,800737690,020,80020C5B0,8630.40,Euro,8630.40,Euro,ACH,1"
  private val cycle1  = "2022/09/01 00:03,01467,8013C4030,020,80BC62F10,58702.10,Yuan,58702.10,Yuan,ACH,1"

  private val sample = List(
    "BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out",
    fanOut1,
    fanOut2,
    "END LAUNDERING ATTEMPT - FAN-OUT",
    "",
    "BEGIN LAUNDERING ATTEMPT - CYCLE:  Max 10 hops",
    cycle1,
    "END LAUNDERING ATTEMPT - CYCLE")

  behavior of "Patterns.parse"

  it should "find each attempt, its scheme and its transactions, in file order" in {
    val attempts = Patterns.parse(sample)
    attempts.map(_.scheme) shouldBe List("FAN-OUT", "CYCLE")
    attempts.head.lines shouldBe List(fanOut1, fanOut2)
    attempts(1).lines shouldBe List(cycle1)
  }

  it should "ignore lines outside any attempt" in {
    Patterns.parse(List("", "not part of an attempt", fanOut1)) shouldBe Nil
  }

  it should "keep an attempt whose END line is missing" in {
    val attempts = Patterns.parse(List(
      "BEGIN LAUNDERING ATTEMPT - FAN-IN:  Max 8-degree Fan-In", fanOut1,
      "BEGIN LAUNDERING ATTEMPT - STACK", cycle1))
    attempts shouldBe List(Attempt("FAN-IN", List(fanOut1)), Attempt("STACK", List(cycle1)))
  }

  behavior of "Patterns.schemeOf"

  it should "drop the details after a colon, and work without one" in {
    Patterns.schemeOf("BEGIN LAUNDERING ATTEMPT - CYCLE:  Max 10 hops") shouldBe "CYCLE"
    Patterns.schemeOf("BEGIN LAUNDERING ATTEMPT - STACK") shouldBe "STACK"
  }
}
