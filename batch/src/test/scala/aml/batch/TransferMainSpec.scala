package aml.batch

import java.sql.Date
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransferMainSpec extends AnyFlatSpec with Matchers {

  "TransferMain.splitDays" should "keep only full days, in order, and split them into learning and test days" in {
    def day(d: Int) = Date.valueOf(f"2022-09-$d%02d")
    val perDay = Seq(day(3) -> 600000L, day(1) -> 500000L, day(11) -> 300L, day(2) -> 400000L, day(4) -> 250000L)

    TransferMain.splitDays(perDay, fullDay = 100000L, trainDays = 2) shouldBe ((Seq(day(1), day(2)), Seq(day(3), day(4))))
  }
}
