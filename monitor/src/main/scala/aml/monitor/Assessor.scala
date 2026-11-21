package aml.monitor

import aml.common.{Reasons, Rule, Rules, ScoringModel}
import scala.io.Source
import scala.util.Using

/** The live monitor's settings: the score above which a payment becomes an alert, and each reason's lift. */
final case class MonitorSettings(threshold: Double, reasonLifts: Map[String, Double])

object MonitorSettings {

  /** Lines `threshold <score>` and `reason <lift> <reason text>`; # starts a comment. */
  def parse(text: String): Either[String, MonitorSettings] = {
    val lines = text.linesIterator.map(_.trim).filter(l => l.nonEmpty && !l.startsWith("#")).toList
    val threshold = lines.collectFirst { case l if l.startsWith("threshold ") => l.stripPrefix("threshold ").trim.toDoubleOption }.flatten
    val lifts = lines.filter(_.startsWith("reason ")).flatMap { l =>
      l.split(" ", 3) match {
        case Array(_, lift, name) => lift.toDoubleOption.map(name -> _)
        case _                    => None
      }
    }.toMap
    threshold.map(MonitorSettings(_, lifts)).toRight("the settings have no threshold line")
  }

  def load(path: String): Either[String, MonitorSettings] = Using.resource(Source.fromFile(path))(s => parse(s.mkString))
}

/** What the monitor decided about one payment: its score, whether it's an alert, the reasons (with their lifts),
  * and how long the decision took, in nanoseconds.
  */
final case class Decision(payment: LivePayment, score: Double, isAlert: Boolean, reasons: Seq[(String, Double)], nanos: Long)

/** Scores each payment with the exported trees, and explains each alert with the kept reasons, strongest first.
  * The same plain-Scala code as the batch side: ScoringModel, Rules.matches and Reasons.render.
  */
final class Assessor(model: ScoringModel, threshold: Double, reasons: Seq[(Rule, Double)]) {

  def assess(p: LivePayment): Decision = assessAt(p, threshold)

  /** The same, with a different alert threshold, which the dashboard's slider can change while it runs. */
  def assessAt(p: LivePayment, alertAt: Double): Decision = {
    val start = System.nanoTime()
    val score = model.score(p.features)
    val alert = score >= alertAt
    val explained =
      if (!alert) Nil
      else reasons.collect { case (r, lift) if Rules.matches(r.condition, p.features) => Reasons.render(r.name, p.features) -> lift }
    Decision(p, score, alert, explained, System.nanoTime() - start)
  }
}

object Assessor {

  /** Keeps the reasons the settings list, ordered by their lift, strongest first. */
  def apply(model: ScoringModel, settings: MonitorSettings, reasons: Seq[Rule]): Assessor =
    new Assessor(model, settings.threshold,
      reasons.flatMap(r => settings.reasonLifts.get(r.name).map(r -> _)).sortBy { case (_, lift) => -lift })
}

/** An alert as one line of JSON, for the dashboard. */
object AlertJson {

  private def quote(s: String): String =
    "\"" + s.flatMap {
      case '"'          => "\\\""
      case '\\'         => "\\\\"
      case c if c < ' ' => f"\\u${c.toInt}%04x"
      case c            => c.toString
    } + "\""

  def of(d: Decision): String = {
    val p       = d.payment
    val reasons = d.reasons.map { case (text, lift) => s"""{"text":${quote(text)},"lift":$lift}""" }.mkString("[", ",", "]")
    val scheme  = p.scheme.map(quote).getOrElse("null")
    val amount  = p.features.get("amountPaidUsd").map(_.toString).getOrElse("null")
    s"""{"txnId":${p.txnId},"time":${quote(p.time)},"sender":${quote(p.sender)},"receiver":${quote(p.receiver)},""" +
      s""""amountUsd":$amount,"format":${quote(p.format)},"score":${d.score},"reasons":$reasons,""" +
      s""""isLaundering":${p.isLaundering},"scheme":$scheme}"""
  }
}
