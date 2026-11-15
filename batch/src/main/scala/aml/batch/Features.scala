package aml.batch

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Account features: what the sending and receiving accounts of each transaction did in the 24 hours before it.
  *
  * Counting each account's earlier payments directly would compare every payment with every other payment of the
  * same account, which is far too slow for busy accounts with thousands of payments a day. So we first add up each
  * account's activity per hour, then look back over at most 24 hourly rows per account.
  *
  * "The 24 hours before" means the 24 whole hours before the transaction's own hour. Earlier payments in the same
  * hour aren't counted: times are only given to the minute, so we can't always tell which came first, and counting
  * a payment that came later would leak the future into the features.
  */
object Features {

  val WindowHours = 24

  /** An account is a bank plus an account number. */
  def accountKey(bank: Column, account: Column): Column = concat_ws("|", bank, account)

  /** The number of whole hours since 1 January 1970 at the time `ts`. */
  def hourOf(ts: Column): Column = floor(unix_timestamp(ts) / 3600).cast("long")

  private val noStrings = typedLit(Seq.empty[String])

  /** One row per account per hour in which it sent or received anything: how much, to or from whom, and how. */
  def hourlyActivity(txns: DataFrame): DataFrame = {
    val sent = txns
      .select(
        accountKey(col("fromBank"), col("fromAcct")).as("account"),
        hourOf(col("ts")).as("hour"),
        accountKey(col("toBank"), col("toAcct")).as("party"),
        col("amountPaidUsd").as("usd"),
        col("payCcy").as("currency"),
        col("format"))
      .groupBy("account", "hour")
      .agg(
        count(lit(1)).as("sentCount"),
        sum("usd").as("sentUsd"),
        max("usd").as("sentMaxUsd"),
        collect_set("party").as("sentTo"),
        collect_set("currency").as("sentCurrencies"),
        collect_set("format").as("sentFormats"))

    val received = txns
      .select(
        accountKey(col("toBank"), col("toAcct")).as("account"),
        hourOf(col("ts")).as("hour"),
        accountKey(col("fromBank"), col("fromAcct")).as("party"),
        col("amountReceivedUsd").as("usd"),
        col("recvCcy").as("currency"),
        col("format"))
      .groupBy("account", "hour")
      .agg(
        count(lit(1)).as("recvCount"),
        sum("usd").as("recvUsd"),
        max("usd").as("recvMaxUsd"),
        collect_set("party").as("recvFrom"),
        collect_set("currency").as("recvCurrencies"),
        collect_set("format").as("recvFormats"))

    sent.join(received, Seq("account", "hour"), "full_outer")
      .withColumn("currencies", array_union(coalesce(col("sentCurrencies"), noStrings), coalesce(col("recvCurrencies"), noStrings)))
      .withColumn("formats", array_union(coalesce(col("sentFormats"), noStrings), coalesce(col("recvFormats"), noStrings)))
      .drop("sentCurrencies", "recvCurrencies", "sentFormats", "recvFormats")
  }

  /** For each account and active hour: its activity in the 24 hours before that hour. */
  def last24Hours(hourly: DataFrame): DataFrame = {
    val before = Window.partitionBy("account").orderBy("hour").rangeBetween(-WindowHours, -1)

    def total(c: String, zero: Any) = coalesce(sum(col(c)).over(before), lit(zero))
    def largest(c: String)          = coalesce(max(col(c)).over(before), lit(0.0))
    def distinctIn(c: String)       = size(array_distinct(flatten(collect_list(col(c)).over(before))))

    hourly.select(
      col("account"),
      col("hour"),
      total("sentCount", 0L).as("sent24hCount"),
      total("sentUsd", 0.0).as("sent24hUsd"),
      largest("sentMaxUsd").as("sent24hMaxUsd"),
      distinctIn("sentTo").as("sent24hParties"),
      total("recvCount", 0L).as("recv24hCount"),
      total("recvUsd", 0.0).as("recv24hUsd"),
      largest("recvMaxUsd").as("recv24hMaxUsd"),
      distinctIn("recvFrom").as("recv24hParties"),
      distinctIn("currencies").as("currencies24h"),
      distinctIn("formats").as("formats24h"))
  }

  /** The transactions, with the 24-hour features of their sender (`sender...`) and receiver (`receiver...`). */
  def withAccountFeatures(txns: DataFrame, last24: DataFrame): DataFrame = {
    val keyed = txns
      .withColumn("senderAccount", accountKey(col("fromBank"), col("fromAcct")))
      .withColumn("receiverAccount", accountKey(col("toBank"), col("toAcct")))
      .withColumn("hour", hourOf(col("ts")))

    keyed
      .join(forRole(last24, "sender"), Seq("senderAccount", "hour"), "left")
      .join(forRole(last24, "receiver"), Seq("receiverAccount", "hour"), "left")
  }

  /** The feature columns: everything starting with sender or receiver, except the two account keys. */
  def featureColumns(featured: DataFrame): Seq[String] =
    featured.columns.toSeq.filter(c => (c.startsWith("sender") || c.startsWith("receiver")) && !c.endsWith("Account"))

  // sent24hCount becomes senderSent24hCount, and so on
  private def forRole(last24: DataFrame, role: String): DataFrame = {
    val features = last24.columns.toSeq.filterNot(Set("account", "hour"))
    last24.select((col("account").as(s"${role}Account") +: col("hour") +: features.map(c => col(c).as(role + c.capitalize))): _*)
  }
}
