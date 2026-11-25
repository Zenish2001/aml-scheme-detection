package aml.common

import java.time.{LocalDateTime, ZoneOffset}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IbmCsvSpec extends AnyFlatSpec with Matchers {

  private val header     = "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering"
  private val currencies = Set("US Dollar", "Euro")
  private def line(time: String, amount: String = "100.00", ccy: String = "US Dollar", flag: String = ",0") =
    s"$time,010,8000EBD30,011,8000EBD31,$amount,$ccy,$amount,$ccy,ACH$flag"

  "IbmCsv.parse" should "read a labelled file into payments in time order, with their accounts and flags" in {
    val p = IbmCsv.parse(Iterator(header, line("2022/09/01 00:25"), line("2022/09/01 00:20", flag = ",1")), currencies, 100).toOption.get

    p.labelled shouldBe true
    p.rows.map(_.payment.minute) shouldBe Seq(
      LocalDateTime.of(2022, 9, 1, 0, 20).toEpochSecond(ZoneOffset.UTC) / 60,
      LocalDateTime.of(2022, 9, 1, 0, 25).toEpochSecond(ZoneOffset.UTC) / 60)
    p.rows.head.isLaundering shouldBe Some(true)
    p.rows.head.payment.sender shouldBe "010|8000EBD30"
    p.rows.head.payment.receiver shouldBe "011|8000EBD31"
    p.first shouldBe "2022-09-01 00:20"
  }

  it should "accept a file without the laundering column, as unlabelled" in {
    val p = IbmCsv.parse(Iterator(line("2022/09/01 00:20", flag = "")), currencies, 100).toOption.get
    p.labelled shouldBe false
    p.rows.head.isLaundering shouldBe None
  }

  it should "skip bad lines and say what's wrong with each, by line number" in {
    val good = (1 to 40).map(i => line(f"2022/09/01 01:$i%02d"))
    val p = IbmCsv.parse(Iterator(header) ++ good.iterator ++ Iterator(line("2022/09/01 02:00", amount = "lots"), line("2022/09/01 02:01", ccy = "Doubloon")), currencies, 100).toOption.get
    p.rows should have size 40
    p.skipped shouldBe 2
    p.problems.head should startWith("line 42: the amount received isn't a number")
    p.problems(1) should include("no exchange rate for the currency 'Doubloon'")
  }

  it should "refuse a file that isn't in IBM's format, or is too big" in {
    IbmCsv.parse(Iterator("a,b,c"), currencies, 100).left.toOption.get should include("has 3 columns")
    IbmCsv.parse(Iterator(line("yesterday"), line("today")), currencies, 100).left.toOption.get should include("None of the payments")
    IbmCsv.parse(Iterator(line("2022/09/01 00:20"), line("2022/09/01 00:21")), currencies, 1).left.toOption.get should include("up to 1 at a time")
  }

  it should "refuse a file whose busiest hour is more than the live monitor can hold" in {
    val burst = Iterator(line("2022/09/01 00:20"), line("2022/09/01 00:40"), line("2022/09/01 01:10"))
    IbmCsv.parse(burst, currencies, 100, maxPerHour = 1).left.toOption.get should include("busiest hour (2022-09-01 00:00) has 2 payments")
  }
}
