package aml.common

import scala.io.Source
import scala.util.Using
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

/** Reads rules written as text, with Scala's parser combinators: small parsers for each piece of the language,
  * combined into a parser for the whole. The grammar, from loosest to tightest binding:
  * {{{
  * rules      = rule*
  * rule       = "rule" "name in quotes" ":" condition
  * condition  = allOf ("or" allOf)*          or binds loosest
  * allOf      = negated ("and" negated)*     and binds tighter, as in arithmetic
  * negated    = "not" negated | simple
  * simple     = "(" condition ")" | comparison
  * comparison = feature (== | != | > | >= | < | <=) value
  * value      = number | "text in quotes"
  * }}}
  * A # starts a comment that runs to the end of the line.
  */
object RuleParser extends RegexParsers {

  override protected val whiteSpace: Regex = """(\s|#[^\n]*)+""".r

  private val keywords = Set("rule", "and", "or", "not")

  /** A keyword, as a whole word: "or" never matches the start of a feature called "orders". */
  private def keyword(word: String): Parser[String] = s"$word\\b".r

  private val feature: Parser[String] =
    """[A-Za-z_][A-Za-z0-9_]*""".r.withFailureMessage("expected a feature name") ^?
      ({ case name if !keywords(name) => name }, name => s"'$name' is a keyword, not a feature name")

  private val op: Parser[Op] =
    Op.all.map(o => literal(o.symbol) ^^^ o).reduce(_ | _)
      .withFailureMessage("expected a comparison: ==, !=, >, >=, < or <=")

  private val quoted: Parser[String] = "\"[^\"\n]*\"".r ^^ (s => s.substring(1, s.length - 1))

  private val value: Parser[Value] =
    ("""-?\d+(\.\d+)?([eE][-+]?\d+)?""".r ^^ (s => Num(s.toDouble)) | quoted ^^ Text)
      .withFailureMessage("expected a number, or text in \"quotes\"")

  private lazy val condition: Parser[Condition] =
    allOf ~ rep(keyword("or") ~> allOf) ^^ { case first ~ rest => rest.foldLeft(first)(Or) }

  private lazy val allOf: Parser[Condition] =
    negated ~ rep(keyword("and") ~> negated) ^^ { case first ~ rest => rest.foldLeft(first)(And) }

  private lazy val negated: Parser[Condition] =
    keyword("not") ~> negated ^^ Not | simple

  private lazy val simple: Parser[Condition] =
    "(" ~> condition <~ ")" | comparison

  private lazy val comparison: Parser[Condition] =
    feature ~ op ~ value ^^ { case f ~ o ~ v => Compare(f, o, v) }

  private lazy val rule: Parser[Rule] =
    keyword("rule") ~> quoted ~ (":" ~> condition) ^^ { case name ~ c => Rule(name, c) }

  /** Every rule in a text, or where it first goes wrong. */
  def parse(text: String): Either[String, List[Rule]] = run(rep(rule), text)

  /** A condition on its own, such as  pairPayments7d == 0 and amountPaidUsd > 5000 */
  def parseCondition(text: String): Either[String, Condition] = run(condition, text)

  def parseFile(path: String): Either[String, List[Rule]] =
    Using.resource(Source.fromFile(path))(source => parse(source.mkString))

  private def run[T](parser: Parser[T], text: String): Either[String, T] = parseAll(parser, text) match {
    case Success(result, _) => Right(result)
    case failure: NoSuccess =>
      val pos = failure.next.pos
      Left(s"line ${pos.line}, column ${pos.column}: ${failure.msg}\n${pos.longString}")
  }
}
