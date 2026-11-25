package aml.common

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.util.Try

/** Part 8d: reads a file of payments in IBM's format, as uploaded to the live monitor, checking every line.
  *
  * Eleven columns, as in IBM's files: time, from bank, account, to bank, account, amount received, receiving
  * currency, amount paid, payment currency, payment format and the laundering flag. The flag is optional; without
  * it the payments are unlabelled. A first line starting with "Timestamp" is taken as the header.
  */
object IbmCsv {

  private val Time    = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm")
  private val Display = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  final case class Row(payment: RawPayment, isLaundering: Option[Boolean])

  /** The payments in time order, whether they're labelled, and the lines that couldn't be read. */
  final case class Parsed(rows: Vector[Row], labelled: Boolean, skipped: Int, problems: Seq[String]) {
    def first: String = rows.headOption.map(r => timeText(r.payment.minute)).getOrElse("")
    def last: String  = rows.lastOption.map(r => timeText(r.payment.minute)).getOrElse("")
  }

  /** Minutes since 1970 (UTC) for a time written like 2022/09/01 00:20. */
  def minuteOf(time: String): Option[Long] =
    Try(LocalDateTime.parse(time.trim, Time).toEpochSecond(ZoneOffset.UTC) / 60).toOption

  /** A minute written as 2022-09-01 00:20, as the dashboard shows times. */
  def timeText(minute: Long): String = LocalDateTime.ofEpochSecond(minute * 60, 0, ZoneOffset.UTC).format(Display)

  /** Reads the lines, skipping and reporting bad ones. The whole file is refused when it can't be IBM's format:
    * a wrong number of columns, no readable payments, more than 1 line in 20 unreadable, more than `maxPayments`,
    * or more than `maxPerHour` payments in one hour (the live monitor keeps every account's recent history in
    * memory, so one enormous hour, like the burst that starts IBM's simulated files, is more than it can hold).
    */
  def parse(lines: Iterator[String], currencies: Set[String], maxPayments: Int, maxProblems: Int = 5,
            maxPerHour: Int = Int.MaxValue): Either[String, Parsed] = {
    val numbered = lines.zipWithIndex.map { case (l, i) => (l.trim, i + 1) }.filter(_._1.nonEmpty).buffered
    if (!numbered.hasNext) return Left("The file is empty.")
    val header = numbered.head._1.toLowerCase.startsWith("timestamp")
    if (header) numbered.next()
    if (!numbered.hasNext) return Left("The file has a header but no payments.")

    val columns = numbered.head._1.split(",", -1).length
    if (columns != 10 && columns != 11)
      return Left(s"Line ${numbered.head._2} has $columns columns, but IBM's format has 11 (or 10, without the laundering column).")
    val labelled = columns == 11

    val rows     = Vector.newBuilder[Row]
    val problems = Vector.newBuilder[String]
    var read, skipped = 0
    numbered.foreach { case (line, n) =>
      read += 1
      readRow(line, n, columns, labelled, currencies) match {
        case Right(row) => rows += row
        case Left(why) =>
          skipped += 1
          if (skipped <= maxProblems) problems += s"line $n: $why"
      }
    }
    val good  = rows.result()
    val shown = problems.result()
    if (good.isEmpty) Left(s"None of the payments could be read, for example: ${shown.mkString("; ")}.")
    else if (skipped * 20 > read) Left(s"$skipped of $read lines couldn't be read, so this doesn't look like IBM's format, for example: ${shown.mkString("; ")}.")
    else if (good.size > maxPayments) Left(f"The file has ${good.size}%,d payments; the monitor checks up to $maxPayments%,d at a time. Try a smaller part of it.")
    else {
      val (busiestHour, inIt) = good.groupMapReduce(_.payment.hour)(_ => 1)(_ + _).maxBy(_._2)
      if (inIt > maxPerHour)
        Left(f"This file's busiest hour (${timeText(busiestHour * 60)}) has $inIt%,d payments, more than the live monitor can hold at once ($maxPerHour%,d). " +
          "Try a file of ordinary days: the first hours of IBM's files are a burst from the simulator starting up.")
      else Right(Parsed(good.sortBy(_.payment.minute), labelled, skipped, shown))
    }
  }

  private def readRow(line: String, n: Int, columns: Int, labelled: Boolean, currencies: Set[String]): Either[String, Row] = {
    val v = line.split(",", -1).map(_.trim)
    def amount(i: Int, what: String) = v(i).toDoubleOption.filter(_ >= 0).toRight(s"the $what isn't a number of 0 or more: '${v(i)}'")
    def currency(i: Int) = if (currencies.contains(v(i))) Right(v(i)) else Left(s"there's no exchange rate for the currency '${v(i)}'")
    def present(i: Int, what: String) = if (v(i).nonEmpty) Right(v(i)) else Left(s"the $what is missing")
    if (v.length != columns) Left(s"it has ${v.length} columns, not $columns")
    else for {
      minute   <- minuteOf(v(0)).toRight(s"the time '${v(0)}' isn't written like 2022/09/01 00:20")
      fromBank <- present(1, "sending bank")
      fromAcct <- present(2, "sending account")
      toBank   <- present(3, "receiving bank")
      toAcct   <- present(4, "receiving account")
      received <- amount(5, "amount received")
      recvCcy  <- currency(6)
      paid     <- amount(7, "amount paid")
      payCcy   <- currency(8)
      format   <- present(9, "payment format")
      flag     <- if (!labelled) Right(None)
                  else v(10) match {
                    case "1" => Right(Some(true))
                    case "0" => Right(Some(false))
                    case other => Left(s"the laundering flag should be 0 or 1, not '$other'")
                  }
    } yield Row(RawPayment(n.toLong, minute, fromBank, fromAcct, toBank, toAcct, received, recvCcy, paid, payCcy, format), flag)
  }
}
