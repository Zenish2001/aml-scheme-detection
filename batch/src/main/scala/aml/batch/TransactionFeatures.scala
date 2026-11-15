package aml.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Part 2b: sharper features, added to the account features of Part 2.
  *
  * Like Part 2, every feature looks only at the past. Where there's nothing to look at (no earlier payments,
  * nothing received), a feature takes a fixed value instead of being empty: -1 for a ratio, and a week for a
  * time gap. Every feature then has a value, which the models in Part 4 need.
  */
object TransactionFeatures {

  val PairWindowHours = 7 * 24
  val MaxMinutes      = 7L * 24 * 60   // "a week or more, or never"
  val NoHistory       = -1.0

  /** The features this object adds. */
  val columns: Seq[String] = Seq(
    "isCrossCurrency", "isSelfTransfer", "isSameBank", "hourOfDay", "isJustUnder10k",
    "pairPayments7d", "senderMinutesSinceReceived", "senderPassThrough24h", "amountVsSenderAverage24h")

  /** All of them, in order. Needs the account features of Part 2, and a unique `txnId` per transaction. */
  def addAll(featured: DataFrame): DataFrame =
    withRatios(withMinutesSinceReceived(withPairHistory(withTransactionFacts(featured))))

  /** Facts about the transaction itself, needing no history. */
  def withTransactionFacts(df: DataFrame): DataFrame =
    df.withColumn("isCrossCurrency", (col("payCcy") =!= col("recvCcy")).cast("int"))
      .withColumn("isSelfTransfer", (col("senderAccount") === col("receiverAccount")).cast("int"))
      .withColumn("isSameBank", (col("fromBank") === col("toBank")).cast("int"))
      .withColumn("hourOfDay", hour(col("ts")))   // the hour of the day only, never the date
      .withColumn("isJustUnder10k", (col("amountPaidUsd") >= 9000 && col("amountPaidUsd") < 10000).cast("int"))

  /** How many times the sender paid this same receiver in the 7 days (168 whole hours) before the transaction's hour.
    * 0 means a new payee, as far as the last week shows.
    */
  def withPairHistory(df: DataFrame): DataFrame = {
    val pairHours = df.groupBy("senderAccount", "receiverAccount", "hour").agg(count(lit(1)).as("payments"))
    val before    = Window.partitionBy("senderAccount", "receiverAccount").orderBy("hour").rangeBetween(-PairWindowHours, -1)
    val history = pairHours.select(
      col("senderAccount"), col("receiverAccount"), col("hour"),
      coalesce(sum("payments").over(before), lit(0L)).as("pairPayments7d"))
    df.join(history, Seq("senderAccount", "receiverAccount", "hour"), "left")
  }

  /** Minutes since the sender last received money, before this transaction's minute; MaxMinutes if a week or more, or never.
    *
    * Unlike the hourly features, this works to the minute: money that leaves 10 minutes after it arrived is exactly
    * the pattern to catch. It's still fast, because each account's payments are scanned once, in time order.
    */
  def withMinutesSinceReceived(df: DataFrame): DataFrame = {
    val seconds  = unix_timestamp(col("ts"))
    val sent     = df.select(col("txnId"), col("senderAccount").as("account"), seconds.as("t"), lit(false).as("received"))
    val received = df.select(lit(null).cast("long").as("txnId"), col("receiverAccount").as("account"), seconds.as("t"), lit(true).as("received"))
    val earlier  = Window.partitionBy("account").orderBy("t").rangeBetween(Window.unboundedPreceding, -1)

    val minutes = sent.unionByName(received)
      .withColumn("lastReceived", max(when(col("received"), col("t"))).over(earlier))
      .filter(!col("received"))
      .select(
        col("txnId"),
        least(coalesce(((col("t") - col("lastReceived")) / 60).cast("long"), lit(MaxMinutes)), lit(MaxMinutes))
          .as("senderMinutesSinceReceived"))

    df.join(minutes, Seq("txnId"), "left")
  }

  /** This payment compared with the sender's last 24 hours; NoHistory (-1) when there's nothing to compare with.
    *
    *  - senderPassThrough24h: (sent in the last 24 hours + this payment) / received in the last 24 hours.
    *    Close to 1 means the sender is passing on what it just received.
    *  - amountVsSenderAverage24h: this payment / the sender's average payment in the last 24 hours.
    */
  def withRatios(df: DataFrame): DataFrame =
    df.withColumn("senderPassThrough24h",
        when(col("senderRecv24hUsd") > 0, (col("senderSent24hUsd") + col("amountPaidUsd")) / col("senderRecv24hUsd"))
          .otherwise(NoHistory))
      .withColumn("amountVsSenderAverage24h",
        when(col("senderSent24hUsd") > 0, col("amountPaidUsd") / (col("senderSent24hUsd") / col("senderSent24hCount")))
          .otherwise(NoHistory))
}
