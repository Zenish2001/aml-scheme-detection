package aml.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.annotation.tailrec

/** Exchange rates, and amounts in US dollars.
  *
  * The simulator behind IBM's dataset converts cross-currency payments at its own fixed rates, which are
  * not real-world ones: rows in HI-Small value a euro at about 1.17 dollars, while in September 2022 a
  * euro was worth about 1 dollar. So we read the rates from the data itself. When a transaction is paid
  * in one currency and received in another, amountReceived / amountPaid is the rate between the two.
  */
object Rates {

  val Usd = "US Dollar"

  /** 1 unit of `from` is worth `rate` units of `to`. */
  final case class PairRate(from: String, to: String, rate: Double)

  /** US dollars per unit of every currency that a chain of pairs connects to US Dollar.
    * Searches outward from US Dollar, so a direct pair with US Dollar is used whenever there is one.
    * Plain Scala with no Spark, so it's quick to test.
    */
  def usdPerUnit(pairs: Seq[PairRate]): Map[String, Double] = {
    // Each pair works both ways: 1 from = rate to, and 1 to = 1/rate from
    val edges: Map[String, Seq[(String, Double)]] =
      (pairs.map(p => p.from -> (p.to -> p.rate)) ++ pairs.map(p => p.to -> (p.from -> 1 / p.rate)))
        .groupMap(_._1)(_._2)

    @tailrec
    def search(queue: List[String], known: Map[String, Double]): Map[String, Double] = queue match {
      case Nil => known
      case currency :: rest =>
        // 1 currency = rate other, so 1 other = known(currency) / rate dollars
        val found = edges.getOrElse(currency, Seq.empty).foldLeft(Map.empty[String, Double]) {
          case (acc, (other, _)) if known.contains(other) || acc.contains(other) => acc
          case (acc, (other, rate))                                              => acc + (other -> known(currency) / rate)
        }
        search(rest ++ found.keys, known ++ found)
    }

    search(List(Usd), Map(Usd -> 1.0))
  }

  /** One row per cross-currency transaction, with its two amounts and the rate it implies:
    * 1 `from` (payCcy) = `rate` `to` (recvCcy).
    */
  def impliedRates(txns: DataFrame): DataFrame =
    txns
      .filter(col("payCcy") =!= col("recvCcy") && col("amountPaid") > 0 && col("amountReceived") > 0)
      .select(
        col("payCcy").as("from"),
        col("recvCcy").as("to"),
        col("amountPaid").as("paid"),
        col("amountReceived").as("received"),
        (col("amountReceived") / col("amountPaid")).as("rate"))

  /** For each currency pair: how many transactions, the 5th, 50th and 95th percentiles of their rates
    * (almost equal if the simulator used fixed rates), and `weightedRate`, total received / total paid.
    *
    * `weightedRate` is the one to use. Amounts are rounded (to the cent, or to 6 decimals for Bitcoin),
    * which distorts the rate of a tiny payment a lot. Adding up the amounts first lets large payments,
    * where rounding doesn't matter, dominate.
    */
  def pairSummary(implied: DataFrame): DataFrame =
    implied.groupBy("from", "to").agg(
      count(lit(1)).as("transactions"),
      percentile_approx(col("rate"), lit(0.05), lit(10000)).as("p05"),
      percentile_approx(col("rate"), lit(0.50), lit(10000)).as("median"),
      percentile_approx(col("rate"), lit(0.95), lit(10000)).as("p95"),
      (sum("received") / sum("paid")).as("weightedRate"))

  /** Adds amountPaidUsd and amountReceivedUsd. A currency missing from `usdPer` gives null, so it can be counted. */
  def withUsd(txns: DataFrame, usdPer: Map[String, Double]): DataFrame = {
    val rates = txns.sparkSession.createDataFrame(usdPer.toSeq).toDF("currency", "usdPerUnit")
    val pay   = rates.select(col("currency").as("payCcy"), col("usdPerUnit").as("payUsdRate"))
    val recv  = rates.select(col("currency").as("recvCcy"), col("usdPerUnit").as("recvUsdRate"))
    txns
      .join(broadcast(pay), Seq("payCcy"), "left")
      .join(broadcast(recv), Seq("recvCcy"), "left")
      .withColumn("amountPaidUsd", col("amountPaid") * col("payUsdRate"))
      .withColumn("amountReceivedUsd", col("amountReceived") * col("recvUsdRate"))
      .drop("payUsdRate", "recvUsdRate")
  }
}
