package aml.monitor

import aml.common.{RuleParser, ScoringModel}
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.{Source => FileSource}

/** Part 7b: the live monitor. Akka Streams plays payments in one at a time, from a replay file; each is scored
  * with the exported trees and, above the threshold, explained and sent to the CaseKeeper actor, which groups
  * alerts into cases. Alerts are also written, one JSON line each, for the dashboard. No Spark anywhere.
  *
  * Run it from the sbt shell, after ReplayExportMain:
  *   monitor/runMain aml.monitor.LiveMonitor                              (HI-Small's test days, as fast as possible)
  *   monitor/runMain aml.monitor.LiveMonitor data/replay/LI-Small         (another dataset)
  *   monitor/runMain aml.monitor.LiveMonitor data/replay/HI-Small 5000    (at 5,000 payments a second)
  */
object LiveMonitor {

  val DefaultReplay = "data/replay/HI-Small"
  val TreesFile     = "data/models/stage1-trees.txt"
  val SettingsFile  = "data/models/monitor-settings.txt"
  val ReasonsFile   = "rules/reasons.rules"
  val AlertsFile    = "data/live/alerts.jsonl"
  val ShownAlerts   = 8

  /** Counts what the stream saw, and how long each decision took. Updated by one stream stage only. */
  final class Tally {
    var payments, alerts, caught, laundering = 0L
    val nanos = mutable.ArrayBuilder.make[Long]
    val days  = mutable.Set.empty[Long]
    def add(d: Decision): Tally = {
      payments += 1
      nanos += d.nanos
      days += d.payment.minute / 1440
      if (d.payment.isLaundering) laundering += 1
      if (d.isAlert) { alerts += 1; if (d.payment.isLaundering) caught += 1 }
      this
    }
  }

  def main(args: Array[String]): Unit = {
    val replay = args.headOption.getOrElse(DefaultReplay)
    val rate   = args.lift(1).flatMap(_.toIntOption).getOrElse(0)   // payments a second; 0 means as fast as possible

    val loaded = for {
      model    <- ScoringModel.load(TreesFile).left.map(e => s"$TreesFile: $e (run ModelExportMain first)")
      settings <- MonitorSettings.load(SettingsFile).left.map(e => s"$SettingsFile: $e (run ReplayExportMain first)")
      reasons  <- RuleParser.parseFile(ReasonsFile).left.map(e => s"$ReasonsFile: $e")
    } yield (Assessor(model, settings, reasons), settings)

    loaded match {
      case Left(error)                 => println(s"\nThe monitor can't start: $error")
      case Right((assessor, settings)) => run(replay, rate, assessor, settings)
    }
  }

  private def run(replay: String, rate: Int, assessor: Assessor, settings: MonitorSettings): Unit = {
    implicit val system: ActorSystem[CaseKeeper.Command] = ActorSystem(CaseKeeper(), "aml-monitor")
    implicit val timeout: Timeout     = 30.seconds

    new File(AlertsFile).getParentFile.mkdirs()
    val alertsOut = new PrintWriter(AlertsFile, "UTF-8")
    val input     = FileSource.fromFile(Replay.file(replay), "UTF-8")

    try {
      val lines = input.getLines()
      val parse = Replay.parser(lines.next())
      val pace = if (rate > 0) f"at $rate%,d payments a second" else "as fast as possible"
      println(s"\nReplaying ${Replay.file(replay)} $pace")
      println(f"Alert threshold ${settings.threshold}%.4f; reasons kept: ${settings.reasonLifts.size}%d. The first alerts:")

      val started = System.nanoTime()
      val payments = Source.fromIterator(() => lines).map(parse)
      val paced    = if (rate > 0) payments.throttle(rate, 1.second) else payments
      val tally = Await.result(
        paced.map(assessor.assess).runWith(Sink.fold(new Tally) { (t, d) =>
          if (d.isAlert) {
            system ! CaseKeeper.Alerted(d)
            alertsOut.println(AlertJson.of(d))
            if (t.alerts < ShownAlerts) show(d)
          }
          t.add(d)
        }),
        Duration.Inf)
      val seconds = (System.nanoTime() - started) / 1e9

      val summary = Await.result(system.ask[CaseKeeper.Summary](ref => CaseKeeper.Report(ref)), timeout.duration)
      report(tally, seconds, summary)
    } finally {
      input.close()
      alertsOut.close()
      system.terminate()
    }
  }

  private def show(d: Decision): Unit = {
    val p      = d.payment
    val amount = p.features.get("amountPaidUsd").collect { case n: java.lang.Number => n.doubleValue }.getOrElse(0.0)
    val scheme = p.scheme.getOrElse("unlisted")
    val truth  = if (p.isLaundering) s"really laundering, $scheme" else "really a false alarm"
    println(f"\n  ${p.time} ${p.sender} paid ${p.receiver} $$$amount%,.0f by ${p.format}: score ${d.score}%.3f ($truth)")
    if (d.reasons.isEmpty) println("    (no reason matched)")
    d.reasons.foreach { case (text, lift) => println(f"    - $text (lift $lift%.1fx)") }
  }

  private def report(t: Tally, seconds: Double, s: CaseKeeper.Summary): Unit = {
    val nanos = t.nanos.result().sorted
    def micros(q: Double) = if (nanos.isEmpty) 0.0 else nanos(math.min(nanos.length - 1, (q * nanos.length).toInt)) / 1000.0
    val days = math.max(t.days.size, 1)

    println(f"\nReplayed ${t.payments}%,d payments in $seconds%.1f s, ${t.payments / seconds}%,.0f a second, reading the file included")
    println(f"Time to decide on one payment (score and reasons): median ${micros(0.5)}%.1f microseconds, " +
      f"95th percentile ${micros(0.95)}%.1f, 99th ${micros(0.99)}%.1f")
    println(f"Alerts: ${t.alerts}%,d over $days%d days (${t.alerts / days}%,d a day); ${t.caught}%,d of them real laundering " +
      f"(${if (t.alerts == 0) 0.0 else t.caught * 100.0 / t.alerts}%.1f%%), catching ${if (t.laundering == 0) 0.0 else t.caught * 100.0 / t.laundering}%.1f%% of the ${t.laundering}%,d laundering payments")
    println("  (the truth is known only because this is a replay of labelled data)")

    println(f"\nCases: ${s.cases.size}%,d, from ${s.alerts}%,d alerts. By shape:")
    s.cases.groupBy(_.scheme).toSeq.sortBy(-_._2.size).foreach { case (scheme, cs) =>
      println("  %-16s %,5d cases, %,5d alerts".format(scheme, cs.size, cs.map(_.alerts.size).sum))
    }
    println("The largest cases:")
    s.cases.take(5).foreach { c =>
      val real = c.alerts.count(_.payment.isLaundering)
      println(s"  case ${c.id}: ${c.scheme}, ${c.alerts.size} alerts ($real real laundering): ${c.why}")
    }
    println(s"\nSaved the alerts, one JSON line each, to $AlertsFile")
  }
}
