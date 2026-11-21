package aml.monitor

import java.util.Locale
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}

/** What the dashboard shows at one moment: running totals, the controls, and the alerts since the threshold
  * last changed (so the page can show what the current threshold does).
  */
final case class Snapshot(
  payments: Long, alerts: Long, caught: Long, laundering: Long, minute: Long, time: String,
  averageMicros: Double, speed: Long, finished: Boolean, source: String, rate: Int, paused: Boolean,
  threshold: Double, defaultThreshold: Double, sinceAlerts: Long, sinceCaught: Long, sinceMinutes: Long)

/** The dashboard's controls, shared between the web server and the stream: pace, pause and the alert threshold. */
final class Controls(initialRate: Int, val defaultThreshold: Double) {
  @volatile var rate: Int         = initialRate
  @volatile var threshold: Double = defaultThreshold

  // A completed promise means "running"; pausing swaps in an unfinished one, and resuming completes it
  private val gate = new AtomicReference[Promise[Unit]](Promise.successful(()))

  def paused: Boolean = !gate.get.isCompleted
  def pause(): Unit = { val g = gate.get; if (g.isCompleted) gate.compareAndSet(g, Promise[Unit]()) }
  def resume(): Unit = gate.get.trySuccess(())

  /** Completes at once when running, or when the stream is resumed. */
  def whenRunning: Future[Unit] = gate.get.future
}

/** The JSON the dashboard page reads. */
object Dashboard {

  /** The features shown when an alert is opened, if the payment has them. */
  val Facts: Seq[String] = Seq("pairPayments7d", "senderMinutesSinceReceived", "receiverRecv24hParties", "senderSent24hParties",
    "amountVsSenderAverage24h", "reciprocal7d", "cyclePaths7d", "scatterGatherPaths7d")

  def quote(s: String): String =
    "\"" + s.flatMap {
      case '"'          => "\\\""
      case '\\'         => "\\\\"
      case c if c < ' ' => "\\u%04x".formatLocal(Locale.US, c.toInt)
      case c            => c.toString
    } + "\""

  private def number(v: Any): String = v match {
    case n: java.lang.Number => n.doubleValue.toString
    case _                   => "null"
  }

  def statsJson(s: Snapshot): String = {
    val micros = "%.2f".formatLocal(Locale.US, s.averageMicros)
    s"""{"payments":${s.payments},"alerts":${s.alerts},"caught":${s.caught},"laundering":${s.laundering},""" +
      s""""minute":${s.minute},"time":${quote(s.time)},"averageMicros":$micros,"speed":${s.speed},""" +
      s""""finished":${s.finished},"source":${quote(s.source)},"rate":${s.rate},"paused":${s.paused},""" +
      s""""threshold":${s.threshold},"defaultThreshold":${s.defaultThreshold},""" +
      s""""sinceAlerts":${s.sinceAlerts},"sinceCaught":${s.sinceCaught},"sinceMinutes":${s.sinceMinutes}}"""
  }

  /** An alert, with its reasons and the facts behind them. */
  def alertJson(d: Decision): String = {
    val p       = d.payment
    val reasons = d.reasons.map { case (text, lift) => s"""{"text":${quote(text)},"lift":$lift}""" }.mkString("[", ",", "]")
    val facts   = Facts.flatMap(f => p.features.get(f).map(v => s"${quote(f)}:${number(v)}")).mkString("{", ",", "}")
    val scheme  = p.scheme.map(quote).getOrElse("null")
    s"""{"txnId":${p.txnId},"time":${quote(p.time)},"sender":${quote(p.sender)},"receiver":${quote(p.receiver)},""" +
      s""""amountUsd":${number(p.features.getOrElse("amountPaidUsd", null))},"format":${quote(p.format)},"score":${d.score},""" +
      s""""reasons":$reasons,"facts":$facts,"isLaundering":${p.isLaundering},"scheme":$scheme}"""
  }

  /** The largest cases first. A case's key is its first alert's transaction, which stays the same as it grows. */
  def casesJson(summary: CaseKeeper.Summary, limit: Int = 25): String =
    summary.cases.take(limit).map { c =>
      s"""{"key":${c.alerts.head.payment.txnId},"id":${c.id},"scheme":${quote(c.scheme)},"why":${quote(c.why)},""" +
        s""""alerts":${c.alerts.size},"real":${c.alerts.count(_.payment.isLaundering)},"first":${quote(c.alerts.head.payment.time)}}"""
    }.mkString(s"""{"total":${summary.cases.size},"cases":[""", ",", "]}")

  /** One case in full, with every payment in it, for drawing its network. */
  def caseJson(c: CaseKeeper.LiveCase): String = {
    val payments = c.alerts.map { d =>
      val p = d.payment
      s"""{"txnId":${p.txnId},"time":${quote(p.time)},"sender":${quote(p.sender)},"receiver":${quote(p.receiver)},""" +
        s""""amountUsd":${number(p.features.getOrElse("amountPaidUsd", null))},"format":${quote(p.format)},""" +
        s""""score":${d.score},"real":${p.isLaundering}}"""
    }.mkString("[", ",", "]")
    s"""{"key":${c.alerts.head.payment.txnId},"scheme":${quote(c.scheme)},"why":${quote(c.why)},"payments":$payments}"""
  }
}
