package aml.batch

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Part 3: graph features, about the shapes money makes as it moves between accounts.
  *
  * For a payment from s (sender) to r (receiver) in hour h, looking only at the 168 whole hours (7 days) before h:
  *  - reciprocal7d:          payments from r back to s (a 2-step loop)
  *  - cyclePaths7d:          accounts x where r paid x, then x paid s, so that s paying r closes a 3-step loop
  *  - scatterGatherPaths7d:  other accounts x where some account p paid both s and x, then x paid r: s is one of
  *                           several go-betweens carrying money from the same source to the same destination
  *  - isBitcoin:             the payment is in Bitcoin
  *  - senderRecvOtherRail24h: payments s received in the last 24 hours through the other rail (Bitcoin or bank)
  *
  * Paths are built from hourly totals, and each step must come in a later hour than the one before, so a path is
  * a real sequence in time. Hub accounts, with more than HubDegree different counterparties, are skipped as
  * go-betweens: they're banks and businesses rather than the mule accounts schemes pass through, and paths
  * through them would multiply into billions.
  */
object GraphFeatures {

  val WindowHours = 7 * 24
  val HubDegree   = 100
  val Bitcoin     = "Bitcoin"

  /** The features this object adds. */
  val columns: Seq[String] = Seq("reciprocal7d", "cyclePaths7d", "scatterGatherPaths7d", "isBitcoin", "senderRecvOtherRail24h")

  /** Accounts with more than HubDegree different counterparties, sending or receiving, anywhere in the data. */
  def hubs(txns: DataFrame): DataFrame = {
    val pairs = txns.filter(col("senderAccount") =!= col("receiverAccount"))
      .select("senderAccount", "receiverAccount").distinct()
    val outgoing = pairs.select(col("senderAccount").as("account"), col("receiverAccount").as("party"))
    val incoming = pairs.select(col("receiverAccount").as("account"), col("senderAccount").as("party"))
    outgoing.union(incoming).distinct()
      .groupBy("account").count()
      .filter(col("count") > HubDegree)
      .select("account")
  }

  /** Payments between different accounts, added up per hour: src paid dst `payments` times in that hour. */
  def edges(txns: DataFrame): DataFrame =
    txns.filter(col("senderAccount") =!= col("receiverAccount"))
      .groupBy(col("senderAccount").as("src"), col("receiverAccount").as("dst"), col("hour"))
      .agg(count(lit(1)).as("payments"))

  /** Two-step paths a -> x -> b, one row per (a, x, b), with every hour a paid x (h1s) and every hour x paid b (h2s).
    * Keeping the hours as lists, rather than one row per pair of hours, keeps the table small; each transaction then
    * checks whether some a -> x came before some x -> b inside its own window. x is never a hub, and a and b differ.
    */
  def twoStepPaths(edges: DataFrame, hubs: DataFrame): DataFrame = {
    val first  = edges.groupBy(col("src").as("a"), col("dst").as("x")).agg(collect_set("hour").as("h1s"))
    val second = edges.groupBy(col("src").as("x"), col("dst").as("b")).agg(collect_set("hour").as("h2s"))
    first
      .join(hubs.withColumnRenamed("account", "x"), Seq("x"), "left_anti")
      .join(second, Seq("x"))
      .filter(col("a") =!= col("b") && array_min(col("h1s")) < array_max(col("h2s")))   // some first step comes before some second step
  }

  /** True when `pastHour` is one of the 168 whole hours before `hour`. */
  def inWindow(pastHour: Column, hour: Column): Column = pastHour.between(hour - WindowHours, hour - 1)

  /** True when some first step (an hour in h1s) comes before some second step (an hour in h2s),
    * both within the 168 whole hours before `hour`.
    */
  def pathInWindow(h1s: Column, h2s: Column, hour: Column): Column =
    exists(h1s, h1 => h1 >= hour - WindowHours && exists(h2s, h2 => h2 > h1 && h2 <= hour - 1))

  /** The transactions with all five graph features. Needs txnId, senderAccount, receiverAccount, hour and format. */
  def withGraphFeatures(txns: DataFrame, hubs: DataFrame): DataFrame = {
    val hourly = edges(txns).cache()
    val paths  = twoStepPaths(hourly, hubs).cache()
    val keys   = txns.select("txnId", "senderAccount", "receiverAccount", "hour")

    // r paid s before
    val reciprocal = keys
      .join(hourly.select(col("src").as("receiverAccount"), col("dst").as("senderAccount"), col("hour").as("pastHour"), col("payments")),
            Seq("senderAccount", "receiverAccount"))
      .filter(inWindow(col("pastHour"), col("hour")))
      .groupBy("txnId").agg(sum("payments").as("reciprocal7d"))

    // r paid x, then x paid s: now s pays r
    val cycles = keys
      .join(paths.select(col("a").as("receiverAccount"), col("b").as("senderAccount"), col("x"), col("h1s"), col("h2s")),
            Seq("senderAccount", "receiverAccount"))
      .filter(pathInWindow(col("h1s"), col("h2s"), col("hour")))
      .groupBy("txnId").agg(countDistinct("x").as("cyclePaths7d"))

    // p paid s, and p paid x, then x paid r. Only for payments between two non-hubs, with p not a hub either.
    val nonHub = keys
      .join(hubs.withColumnRenamed("account", "senderAccount"), Seq("senderAccount"), "left_anti")
      .join(hubs.withColumnRenamed("account", "receiverAccount"), Seq("receiverAccount"), "left_anti")
    val payers = hourly
      .join(hubs.withColumnRenamed("account", "src"), Seq("src"), "left_anti")
      .groupBy(col("src").as("p"), col("dst").as("senderAccount")).agg(collect_set("hour").as("hps"))
    val scatter = nonHub
      .join(payers, Seq("senderAccount"))
      .filter(exists(col("hps"), hp => inWindow(hp, col("hour"))))
      .drop("hps")
      .join(paths.select(col("a").as("p"), col("x"), col("b").as("receiverAccount"), col("h1s"), col("h2s")),
            Seq("p", "receiverAccount"))
      .filter(pathInWindow(col("h1s"), col("h2s"), col("hour")) && col("x") =!= col("senderAccount"))
      .groupBy("txnId").agg(countDistinct("x").as("scatterGatherPaths7d"))

    val withPaths = txns
      .join(reciprocal, Seq("txnId"), "left")
      .join(cycles, Seq("txnId"), "left")
      .join(scatter, Seq("txnId"), "left")
      .withColumn("reciprocal7d", coalesce(col("reciprocal7d"), lit(0L)))
      .withColumn("cyclePaths7d", coalesce(col("cyclePaths7d"), lit(0L)))
      .withColumn("scatterGatherPaths7d", coalesce(col("scatterGatherPaths7d"), lit(0L)))
      .withColumn("isBitcoin", (col("format") === Bitcoin).cast("int"))

    withCrossRail(withPaths)
  }

  /** Payments the sender received in the last 24 whole hours through the other rail: bank payments
    * if this payment is in Bitcoin, and Bitcoin payments if it isn't.
    */
  def withCrossRail(txns: DataFrame): DataFrame = {
    val receipts = txns.select(
      col("receiverAccount").as("account"), col("hour"),
      (col("format") === Bitcoin).cast("long").as("bitcoin"), (col("format") =!= Bitcoin).cast("long").as("bank"))
    val sends  = txns.select(col("senderAccount").as("account"), col("hour"), lit(0L).as("bitcoin"), lit(0L).as("bank"))
    val hourly = receipts.unionByName(sends).groupBy("account", "hour").agg(sum("bitcoin").as("bitcoin"), sum("bank").as("bank"))
    val before = Window.partitionBy("account").orderBy("hour").rangeBetween(-Features.WindowHours, -1)
    val last24 = hourly.select(
      col("account").as("senderAccount"), col("hour"),
      coalesce(sum("bitcoin").over(before), lit(0L)).as("bitcoinRecv24h"),
      coalesce(sum("bank").over(before), lit(0L)).as("bankRecv24h"))

    txns.join(last24, Seq("senderAccount", "hour"), "left")
      .withColumn("senderRecvOtherRail24h", when(col("format") === Bitcoin, col("bankRecv24h")).otherwise(col("bitcoinRecv24h")))
      .drop("bitcoinRecv24h", "bankRecv24h")
  }
}
