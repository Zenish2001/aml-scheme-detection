package aml.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import Rates.{PairRate, Usd}

class RatesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("RatesSpec")

  override def afterAll(): Unit = spark.stop()

  private val tolerance = 1e-9

  behavior of "Rates.usdPerUnit"

  it should "use a pair with US Dollar in either direction" in {
    val usdPer = Rates.usdPerUnit(Seq(PairRate("Euro", Usd, 1.1718), PairRate(Usd, "Rupee", 73.44)))
    usdPer(Usd) shouldBe 1.0
    usdPer("Euro") shouldBe (1.1718 +- tolerance)
    usdPer("Rupee") shouldBe (1 / 73.44 +- tolerance)
  }

  it should "chain through another currency when there's no direct pair with US Dollar" in {
    // 1 Canadian Dollar = 0.8 US Dollar and 59 Ruble, so 1 Ruble = 0.8 / 59 US Dollar
    val usdPer = Rates.usdPerUnit(Seq(PairRate("Canadian Dollar", Usd, 0.8), PairRate("Canadian Dollar", "Ruble", 59.0)))
    usdPer("Ruble") shouldBe (0.8 / 59.0 +- tolerance)
  }

  it should "leave out a currency that no pair connects to US Dollar" in {
    Rates.usdPerUnit(Seq(PairRate("Yen", "Yuan", 0.05))) shouldBe Map(Usd -> 1.0)
  }

  // Paid 100 euros and received 117.18 dollars; a dollar payment; and a yen payment with no rate
  private def txns: DataFrame = {
    val session = spark
    import session.implicits._
    Seq(("Euro", 100.0, Usd, 117.18), (Usd, 50.0, Usd, 50.0), ("Yen", 1000.0, "Yen", 1000.0))
      .toDF("payCcy", "amountPaid", "recvCcy", "amountReceived")
  }

  behavior of "Rates.impliedRates"

  it should "find the rate implied by each cross-currency transaction" in {
    val rows = Rates.impliedRates(txns).collect()
    rows should have length 1
    rows.head.getAs[String]("from") shouldBe "Euro"
    rows.head.getAs[String]("to") shouldBe Usd
    rows.head.getAs[Double]("rate") shouldBe (1.1718 +- tolerance)
  }

  behavior of "Rates.pairSummary"

  it should "weight the rate by amount, so tiny rounded payments can't distort it" in {
    val session = spark
    import session.implicits._
    // 1,000 euros at the true rate, and a 1-cent payment whose rate rounding has distorted to 1.0
    val implied = Rates.impliedRates(
      Seq(("Euro", 1000.0, Usd, 1171.78), ("Euro", 0.01, Usd, 0.01)).toDF("payCcy", "amountPaid", "recvCcy", "amountReceived"))
    val row = Rates.pairSummary(implied).collect().head

    row.getAs[Long]("transactions") shouldBe 2
    row.getAs[Double]("weightedRate") shouldBe (1.1718 +- 1e-4)
  }

  behavior of "Rates.withUsd"

  it should "convert both amounts to US dollars, with null for a currency it has no rate for" in {
    val converted = Rates.withUsd(txns, Map(Usd -> 1.0, "Euro" -> 1.1718))
    val paidUsd = converted.collect().map { row =>
      row.getAs[String]("payCcy") -> Option(row.getAs[java.lang.Double]("amountPaidUsd")).map(_.doubleValue)
    }.toMap

    converted.count() shouldBe 3   // converting adds and drops no rows
    paidUsd("Euro").get shouldBe (117.18 +- tolerance)
    paidUsd(Usd).get shouldBe (50.0 +- tolerance)
    paidUsd("Yen") shouldBe None
  }
}
