package aml.common

import java.util.Locale
import scala.util.matching.Regex

/** Reasons explain an alert in plain English. A reason is a Rule whose name is the text an analyst reads, such as
  * "Money passed on {senderMinutesSinceReceived} minutes after it arrived"; each {feature} is filled in with that
  * transaction's value. Plain Scala, with no Spark, so the live monitor (Part 7) can explain its alerts too.
  */
object Reasons {

  private val Placeholder: Regex = """\{([A-Za-z_][A-Za-z0-9_]*)\}""".r

  /** The features a reason's text refers to. */
  def placeholders(text: String): List[String] = Placeholder.findAllMatchIn(text).map(_.group(1)).toList

  /** A reason's text, with each {feature} replaced by this transaction's value. */
  def render(text: String, row: Map[String, Any]): String =
    Placeholder.replaceAllIn(text, m => {
      val feature = m.group(1)
      val value   = row.getOrElse(feature, throw new NoSuchElementException(s"no value for feature $feature"))
      Regex.quoteReplacement(show(value))
    })

  /** Every reason this transaction matches, as text, in the order the reasons are given. */
  def explain(reasons: Seq[Rule], row: Map[String, Any]): List[String] =
    reasons.filter(reason => Rules.matches(reason.condition, row)).map(reason => render(reason.name, row)).toList

  /** Numbers the way a person writes them: 12, 4.2, 12,500. */
  def show(value: Any): String = value match {
    case n: java.lang.Number =>
      val v = n.doubleValue
      if (math.abs(v) >= 100) "%,.0f".formatLocal(Locale.US, v)
      else if (v.isWhole) v.toLong.toString
      else "%.1f".formatLocal(Locale.US, v)
    case other => String.valueOf(other)
  }
}
