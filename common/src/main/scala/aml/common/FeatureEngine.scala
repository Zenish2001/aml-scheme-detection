package aml.common

import scala.collection.mutable

/** One payment as a file gives it, with its time as whole minutes since 1 January 1970 (UTC). */
final case class RawPayment(
  id: Long, minute: Long, fromBank: String, fromAcct: String, toBank: String, toAcct: String,
  amountReceived: Double, recvCcy: String, amountPaid: Double, payCcy: String, format: String) {
  val sender: String   = fromBank + "|" + fromAcct
  val receiver: String = toBank + "|" + toAcct
  def hour: Long       = Math.floorDiv(minute, 60L)
}

/** Part 8c: the 34 features of Parts 2, 2b and 3, computed in plain Scala, one hour of payments at a time, so the
  * live monitor can score payments it has never seen without Spark. It reproduces the Spark definitions exactly:
  *
  *  - account features: the sender's and receiver's activity in the 24 whole hours before the payment's hour
  *  - pair history and graph features: the 168 whole hours (7 days) before it, with paths in time order and
  *    hub accounts skipped as go-betweens
  *  - minutes since the sender last received money: to the minute, counting only earlier minutes
  *
  * Payments must arrive in time order. Every feature of a payment in hour H is computed from hours before H
  * (or, for the minutes feature, minutes before its own), and only then is hour H remembered, so no payment
  * ever sees the future. History older than a week is forgotten as time moves on.
  */
final class FeatureEngine(usdPer: Map[String, Double], hubs: Set[String]) {
  import FeatureEngine._

  // Each account's activity per hour, for the last 24 hours
  private val activity = mutable.HashMap.empty[String, mutable.ArrayDeque[AccountHour]]
  // Payments per hour between each sender and receiver (self-transfers included), for the last 168 hours
  private val pairs = mutable.HashMap.empty[(String, String), mutable.ArrayDeque[HourCount]]
  // The same counts between different accounts, indexed both ways for finding paths
  private val out = mutable.HashMap.empty[String, mutable.HashMap[String, mutable.ArrayDeque[HourCount]]]
  private val in  = mutable.HashMap.empty[String, mutable.HashMap[String, mutable.ArrayDeque[HourCount]]]
  // When each account last received money (a minute)
  private val lastReceived = mutable.HashMap.empty[String, Long]
  private var nextSweep    = Long.MinValue

  def usdOf(amount: Double, currency: String): Double =
    amount * usdPer.getOrElse(currency, throw new IllegalArgumentException(s"no US-dollar rate for $currency"))

  /** The features of one hour's payments, all in the same hour and in minute order, one payment at a time, as
    * they're asked for; once the last one has been handed back, it remembers the hour. Handing them back one by one
    * keeps memory small even for a huge hour, such as the burst that starts IBM's files.
    */
  def hour(payments: Seq[RawPayment]): Iterator[(RawPayment, Map[String, Double])] = {
    if (payments.isEmpty) return Iterator.empty
    val h = payments.head.hour
    require(payments.forall(_.hour == h), "payments of one call must share an hour")
    sweep(h)

    val windows = mutable.HashMap.empty[String, Window24]   // each account's 24 hours, once per hour
    def window(account: String) = windows.getOrElseUpdate(account, last24(account, h))

    // Minutes since received: earlier minutes count, even within this hour, so receipts are noted minute by minute
    var minuteNow = Long.MinValue
    val receivedThisMinute = mutable.ArrayBuffer.empty[String]
    def noteReceipts(): Unit = { receivedThisMinute.foreach(r => lastReceived(r) = minuteNow); receivedThisMinute.clear() }

    def features(p: RawPayment): (RawPayment, Map[String, Double]) = {
      if (p.minute != minuteNow) { noteReceipts(); minuteNow = p.minute }
      val minutesSince = lastReceived.get(p.sender).map(r => math.min(p.minute - r, MaxMinutes).toDouble).getOrElse(MaxMinutes.toDouble)
      receivedThisMinute += p.receiver

      val s = window(p.sender)
      val r = window(p.receiver)
      val paidUsd = usdOf(p.amountPaid, p.payCcy)
      val f = mutable.LinkedHashMap.empty[String, Double]
      s.into("sender", f)
      r.into("receiver", f)
      f("isCrossCurrency") = flag(p.payCcy != p.recvCcy)
      f("isSelfTransfer")  = flag(p.sender == p.receiver)
      f("isSameBank")      = flag(p.fromBank == p.toBank)
      f("hourOfDay")       = Math.floorMod(h, 24L).toDouble
      f("isJustUnder10k")  = flag(paidUsd >= 9000 && paidUsd < 10000)
      f("pairPayments7d")  = countIn(pairs.get((p.sender, p.receiver)), h - WeekHours, h - 1).toDouble
      f("senderMinutesSinceReceived") = minutesSince
      f("senderPassThrough24h")     = if (s.recvUsd > 0) (s.sentUsd + paidUsd) / s.recvUsd else NoHistory
      f("amountVsSenderAverage24h") = if (s.sentUsd > 0) paidUsd / (s.sentUsd / s.sentCount) else NoHistory
      f("reciprocal7d")         = if (p.sender == p.receiver) 0.0 else countIn(pairs.get((p.receiver, p.sender)), h - WeekHours, h - 1).toDouble
      f("cyclePaths7d")         = cycles(p.sender, p.receiver, h).toDouble
      f("scatterGatherPaths7d") = scatterGather(p.sender, p.receiver, h).toDouble
      f("isBitcoin")            = flag(p.format == Bitcoin)
      f("senderRecvOtherRail24h") = (if (p.format == Bitcoin) s.bankRecv else s.bitcoinRecv).toDouble
      f("amountPaidUsd")        = paidUsd
      p -> f.toMap
    }

    // Nothing of this hour is remembered until every payment in it has its features, so none sees its own hour
    val each = payments.iterator.map(features)
    var done = false
    new Iterator[(RawPayment, Map[String, Double])] {
      def hasNext: Boolean = {
        val more = each.hasNext
        if (!more && !done) { done = true; noteReceipts(); payments.foreach(remember(_, h)) }
        more
      }
      def next(): (RawPayment, Map[String, Double]) = each.next()
    }
  }

  private def flag(b: Boolean): Double = if (b) 1.0 else 0.0

  /** The sum of counts with an hour from `from` to `to`, inclusive. */
  private def countIn(hours: Option[mutable.ArrayDeque[HourCount]], from: Long, to: Long): Long =
    hours.map(_.iterator.filter(c => c.hour >= from && c.hour <= to).map(_.count).sum).getOrElse(0L)

  /** The earliest hour from `from` on, and the latest hour up to `to`, of a pair's payments. */
  private def earliestFrom(hours: mutable.ArrayDeque[HourCount], from: Long): Option[Long] =
    hours.iterator.map(_.hour).find(_ >= from)
  private def latestUpTo(hours: mutable.ArrayDeque[HourCount], to: Long): Option[Long] =
    hours.reverseIterator.map(_.hour).find(_ <= to)

  /** a paid x, then x paid b, both within the week before hour h, the first before the second. */
  private def pathInWindow(ax: mutable.ArrayDeque[HourCount], xb: mutable.ArrayDeque[HourCount], h: Long): Boolean =
    (earliestFrom(ax, h - WeekHours), latestUpTo(xb, h - 1)) match {
      case (Some(h1), Some(h2)) => h1 < h2
      case _                    => false
    }

  /** Go-betweens x, not hubs, through which money went r -> x -> s: s paying r closes the loop. */
  private def cycles(s: String, r: String, h: Long): Int =
    if (s == r) 0
    else (out.get(r), in.get(s)) match {
      case (Some(fromR), Some(intoS)) =>
        val (small, large, rFirst) = if (fromR.size <= intoS.size) (fromR, intoS, true) else (intoS, fromR, false)
        small.count { case (x, hoursA) =>
          !hubs.contains(x) && large.get(x).exists { hoursB =>
            if (rFirst) pathInWindow(hoursA, hoursB, h) else pathInWindow(hoursB, hoursA, h)
          }
        }
      case _ => 0
    }

  /** Other go-betweens x (not s, not hubs): some non-hub p paid s in the week, and money went p -> x -> r.
    * Only for payments between two non-hubs.
    */
  private def scatterGather(s: String, r: String, h: Long): Int =
    if (hubs.contains(s) || hubs.contains(r)) 0
    else {
      val intoR = in.getOrElse(r, Empty)
      val xs = mutable.HashSet.empty[String]
      in.getOrElse(s, Empty).foreach { case (p, ps) =>
        if (p != r && !hubs.contains(p) && countIn(Some(ps), h - WeekHours, h - 1) > 0) {
          val fromP = out.getOrElse(p, Empty)
          val (small, large, pFirst) = if (fromP.size <= intoR.size) (fromP, intoR, true) else (intoR, fromP, false)
          small.foreach { case (x, hoursA) =>
            if (x != s && !hubs.contains(x) && !xs.contains(x)) large.get(x).foreach { hoursB =>
              if (if (pFirst) pathInWindow(hoursA, hoursB, h) else pathInWindow(hoursB, hoursA, h)) xs += x
            }
          }
        }
      }
      xs.size
    }

  /** An account's activity in the 24 whole hours before hour h. */
  private def last24(account: String, h: Long): Window24 = {
    val inWindow = (a: AccountHour) => a.hour >= h - DayHours && a.hour <= h - 1
    activity.get(account) match {
      case Some(hours) if hours.exists(inWindow) =>
        val w = new Window24
        hours.foreach(a => if (inWindow(a)) w.add(a))
        w
      case _ => NoActivity   // most accounts, in a busy first hour: one shared, empty window, only ever read
    }
  }

  /** Adds one payment to the history, as part of hour h. */
  private def remember(p: RawPayment, h: Long): Unit = {
    val paidUsd = usdOf(p.amountPaid, p.payCcy)
    val recvUsd = usdOf(p.amountReceived, p.recvCcy)
    hourOf(p.sender, h).sent(p.receiver, paidUsd, p.payCcy, p.format)
    hourOf(p.receiver, h).received(p.sender, recvUsd, p.recvCcy, p.format)

    val counts = pairs.getOrElseUpdate((p.sender, p.receiver), mutable.ArrayDeque.empty[HourCount])
    if (counts.nonEmpty && counts.last.hour == h) counts.last.count += 1 else counts += new HourCount(h, 1)
    if (p.sender != p.receiver) {
      out.getOrElseUpdate(p.sender, mutable.HashMap.empty).getOrElseUpdate(p.receiver, counts)
      in.getOrElseUpdate(p.receiver, mutable.HashMap.empty).getOrElseUpdate(p.sender, counts)
    }
  }

  private def hourOf(account: String, h: Long): AccountHour = {
    val hours = activity.getOrElseUpdate(account, mutable.ArrayDeque.empty[AccountHour])
    if (hours.nonEmpty && hours.last.hour == h) hours.last
    else { val a = new AccountHour(h); hours += a; a }
  }

  /** Forgets history older than a week (a day for account activity), once per simulated day. */
  private def sweep(h: Long): Unit =
    if (h >= nextSweep) {
      nextSweep = h + DayHours
      activity.filterInPlace { case (_, hours) => hours.dropWhileInPlace(_.hour < h - DayHours); hours.nonEmpty }
      pairs.filterInPlace { case (_, hours) => hours.dropWhileInPlace(_.hour < h - WeekHours); hours.nonEmpty }
      Seq(out, in).foreach(_.filterInPlace { case (_, peers) => peers.filterInPlace { case (_, hours) => hours.nonEmpty }; peers.nonEmpty })
      lastReceived.filterInPlace { case (_, m) => m >= h * 60 - MaxMinutes }
    }
}

object FeatureEngine {

  val DayHours   = 24L
  val WeekHours  = 7L * 24
  val MaxMinutes = 7L * 24 * 60   // "a week or more, or never"
  val NoHistory  = -1.0
  val HubDegree  = 100
  val Bitcoin    = "Bitcoin"

  private val Empty = mutable.HashMap.empty[String, mutable.ArrayDeque[HourCount]]

  final class HourCount(val hour: Long, var count: Long)

  /** One account's activity in one hour. */
  final class AccountHour(val hour: Long) {
    var sentCount, recvCount, bitcoinRecv, bankRecv = 0L
    var sentUsd, recvUsd = 0.0
    var sentMax, recvMax = Double.NaN
    val sentTo, recvFrom, currencies, formats = mutable.HashSet.empty[String]
    def sent(to: String, usd: Double, currency: String, format: String): Unit = {
      sentCount += 1; sentUsd += usd; sentMax = if (sentMax.isNaN) usd else math.max(sentMax, usd)
      sentTo += to; currencies += currency; formats += format
    }
    def received(from: String, usd: Double, currency: String, format: String): Unit = {
      recvCount += 1; recvUsd += usd; recvMax = if (recvMax.isNaN) usd else math.max(recvMax, usd)
      recvFrom += from; currencies += currency; formats += format
      if (format == Bitcoin) bitcoinRecv += 1 else bankRecv += 1
    }
  }

  /** An account's activity added up over a 24-hour window. */
  final class Window24 {
    var sentCount, recvCount, bitcoinRecv, bankRecv = 0L
    var sentUsd, recvUsd = 0.0
    var sentMax, recvMax = 0.0
    val sentTo, recvFrom, currencies, formats = mutable.HashSet.empty[String]
    def add(a: AccountHour): Unit = {
      sentCount += a.sentCount; recvCount += a.recvCount; bitcoinRecv += a.bitcoinRecv; bankRecv += a.bankRecv
      sentUsd += a.sentUsd; recvUsd += a.recvUsd
      if (!a.sentMax.isNaN) sentMax = math.max(sentMax, a.sentMax)
      if (!a.recvMax.isNaN) recvMax = math.max(recvMax, a.recvMax)
      sentTo ++= a.sentTo; recvFrom ++= a.recvFrom; currencies ++= a.currencies; formats ++= a.formats
    }
    def into(role: String, f: mutable.Map[String, Double]): Unit = {
      f(role + "Sent24hCount") = sentCount.toDouble
      f(role + "Sent24hUsd") = sentUsd
      f(role + "Sent24hMaxUsd") = sentMax
      f(role + "Sent24hParties") = sentTo.size.toDouble
      f(role + "Recv24hCount") = recvCount.toDouble
      f(role + "Recv24hUsd") = recvUsd
      f(role + "Recv24hMaxUsd") = recvMax
      f(role + "Recv24hParties") = recvFrom.size.toDouble
      f(role + "Currencies24h") = currencies.size.toDouble
      f(role + "Formats24h") = formats.size.toDouble
    }
  }

  /** The window of an account with no activity in the 24 hours: shared by all of them, and never added to. */
  private val NoActivity = new Window24

  /** Accounts with more than HubDegree different counterparties, sending or receiving, anywhere in the payments. */
  def hubs(payments: Iterator[RawPayment]): Set[String] = {
    val parties = mutable.HashMap.empty[String, mutable.HashSet[String]]
    payments.foreach { p =>
      if (p.sender != p.receiver) {
        parties.getOrElseUpdate(p.sender, mutable.HashSet.empty) += p.receiver
        parties.getOrElseUpdate(p.receiver, mutable.HashSet.empty) += p.sender
      }
    }
    parties.collect { case (account, ps) if ps.size > HubDegree => account }.toSet
  }

  /** Every payment with its features, in order. The payments must be in time order. */
  def run(payments: Iterator[RawPayment], usdPer: Map[String, Double], hubs: Set[String]): Iterator[(RawPayment, Map[String, Double])] = {
    val engine = new FeatureEngine(usdPer, hubs)
    val input  = payments.buffered
    // One hour of payments at a time, for as long as there are payments
    Iterator.continually(()).takeWhile(_ => input.hasNext).map { _ =>
      val h       = input.head.hour
      val oneHour = mutable.ArrayBuffer.empty[RawPayment]
      while (input.hasNext && input.head.hour == h) {
        val p = input.next()
        require(oneHour.isEmpty || p.minute >= oneHour.last.minute, "payments must be in time order")
        oneHour += p
      }
      engine.hour(oneHour.toSeq)
    }.flatten
  }
}
