package aml.monitor

import java.util.Locale

/** Part 8b: a SAR-style summary of one case: the details an analyst needs to write a Suspicious Activity Report
  * (accounts, banks, payments, total, dates, payment formats and reasons), plus a paragraph in plain words.
  * `real` is the truth from IBM's labels, known only in a replay, and kept out of the paragraph.
  */
final case class SarSummary(
  caseId: Int, scheme: String, shape: String, accounts: Seq[String], banks: Seq[String], payments: Int,
  totalUsd: Double, first: String, last: String, formats: Seq[(String, Int)], reasons: Seq[(String, Int)], real: Int) {

  def narrative: String = {
    val span     = if (first == last) s"On $first" else s"Between $first and $last"
    val bankWord = if (banks.size == 1) "bank" else "banks"
    val payWord  = if (payments == 1) "payment" else "payments"
    val rails    = formats.map { case (f, n) => s"$n by $f" }.mkString(", ")
    val why =
      if (reasons.isEmpty) "no listed reason matched, so the model flagged them from a combination of their features"
      else reasons.take(3).map { case (text, n) => s"$text ($n of $payments)" }.mkString("; ")
    s"$span, ${accounts.size} accounts at ${banks.size} $bankWord moved ${Sar.money(totalUsd)} in $payments $payWord ($rails). " +
      s"Their pattern looks like a ${scheme.toLowerCase}: $shape. The alerts were raised because: $why."
  }
}

object Sar {

  def money(x: Double): String = "$%,.0f".formatLocal(Locale.US, x)

  /** An account is written bank|account, so its bank is what comes before the bar. */
  def bankOf(account: String): String = account.takeWhile(_ != '|')

  private def amountOf(d: Decision): Double =
    d.payment.features.get("amountPaidUsd").collect { case n: java.lang.Number => n.doubleValue }.getOrElse(0.0)

  def of(c: CaseKeeper.LiveCase): SarSummary = {
    val ps       = c.alerts.map(_.payment)
    val accounts = ps.flatMap(p => Seq(p.sender, p.receiver)).distinct.sorted
    val times    = ps.map(_.time).sorted
    val formats  = ps.groupBy(_.format).toSeq.map { case (f, xs) => f -> xs.size }.sortBy { case (f, n) => (-n, f) }
    // The same reason can read differently from payment to payment (its numbers), so reasons are grouped by their lift
    val reasons = c.alerts.flatMap(_.reasons).groupBy { case (_, lift) => lift }.toSeq
      .map { case (_, rs) => rs.head._1 -> rs.size }.sortBy { case (text, n) => (-n, text) }
    SarSummary(c.id, c.scheme, c.why, accounts, accounts.map(bankOf).distinct.sorted, ps.size, c.alerts.map(amountOf).sum,
      times.headOption.getOrElse(""), times.lastOption.getOrElse(""), formats, reasons, ps.count(_.isLaundering))
  }

  /** The summary as a few lines of plain text. */
  def report(s: SarSummary): String = {
    val formats = s.formats.map { case (f, n) => s"$f $n" }.mkString(", ")
    val reasons = if (s.reasons.isEmpty) "none matched" else s.reasons.map { case (t, n) => s"$t ($n)" }.mkString("; ")
    Seq(
      s"  Case ${s.caseId}: ${s.scheme}",
      s"  Accounts:        ${s.accounts.size}, at ${s.banks.size} banks",
      s"  Payments:        ${s.payments}, ${money(s.totalUsd)} in all",
      s"  Dates:           ${s.first} to ${s.last}",
      s"  Payment formats: $formats",
      s"  Reasons:         $reasons",
      s"  Summary:         ${s.narrative}").mkString("\n")
  }
}
