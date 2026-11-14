package aml.batch

/** One laundering attempt from the patterns file: its scheme, and its transactions as CSV lines. */
final case class Attempt(scheme: String, lines: List[String])

/** Reads IBM's patterns file, where each laundering attempt looks like this:
  * {{{
  * BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out
  * 2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1
  * ...
  * END LAUNDERING ATTEMPT - FAN-OUT
  * }}}
  * Plain Scala with no Spark, so it's quick to test.
  */
object Patterns {

  val Begin = "BEGIN LAUNDERING ATTEMPT"
  val End   = "END LAUNDERING ATTEMPT"

  /** The scheme named in a BEGIN line, without the details after the colon:
    * "FAN-OUT" from "BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out".
    */
  def schemeOf(header: String): String =
    header.stripPrefix(Begin).dropWhile(c => !c.isLetter).takeWhile(_ != ':').trim

  /** Groups the file's lines into attempts, in file order, skipping anything outside an attempt.
    * An attempt whose END line is missing still counts: it ends at the next BEGIN line or at the end of the file.
    */
  def parse(lines: List[String]): List[Attempt] = {
    val (finished, stillOpen) = lines.foldLeft((List.empty[Attempt], Option.empty[Attempt])) {
      case ((done, open), line) if line.startsWith(Begin) =>
        (open.map(finish).toList ::: done, Some(Attempt(schemeOf(line), Nil)))
      case ((done, Some(attempt)), line) if line.startsWith(End) =>
        (finish(attempt) :: done, None)
      case ((done, Some(attempt)), line) if line.trim.nonEmpty =>
        (done, Some(attempt.copy(lines = line.trim :: attempt.lines)))
      case (state, _) =>
        state
    }
    (stillOpen.map(finish).toList ::: finished).reverse
  }

  // Lines are collected newest first, so put them back in file order
  private def finish(attempt: Attempt): Attempt = attempt.copy(lines = attempt.lines.reverse)
}
