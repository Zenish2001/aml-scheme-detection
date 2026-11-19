package aml.common

/** A comparison operator, such as > or ==. */
sealed abstract class Op(val symbol: String) {
  /** Whether the operator holds, given how the left side compares with the right: negative, zero or positive. */
  def holds(comparison: Int): Boolean
}

object Op {
  case object Eq extends Op("==") { def holds(c: Int): Boolean = c == 0 }
  case object Ne extends Op("!=") { def holds(c: Int): Boolean = c != 0 }
  case object Ge extends Op(">=") { def holds(c: Int): Boolean = c >= 0 }
  case object Le extends Op("<=") { def holds(c: Int): Boolean = c <= 0 }
  case object Gt extends Op(">")  { def holds(c: Int): Boolean = c > 0 }
  case object Lt extends Op("<")  { def holds(c: Int): Boolean = c < 0 }

  /** Two-character operators first, so that ">=" is never read as ">" followed by "=". */
  val all: List[Op] = List(Eq, Ne, Ge, Le, Gt, Lt)
}

/** The right-hand side of a comparison: a number, or text in quotes. */
sealed trait Value
final case class Num(value: Double) extends Value
final case class Text(value: String) extends Value

/** A rule's condition, as a tree: comparisons joined by and, or and not. */
sealed trait Condition
final case class Compare(feature: String, op: Op, value: Value) extends Condition
final case class And(left: Condition, right: Condition) extends Condition
final case class Or(left: Condition, right: Condition) extends Condition
final case class Not(condition: Condition) extends Condition

/** A named rule, such as  rule "New payee, over $5,000": pairPayments7d == 0 and amountPaidUsd > 5000 */
final case class Rule(name: String, condition: Condition)

/** Working with rules in plain Scala, with no Spark, so the live monitor (Part 7) can use them too. */
object Rules {

  /** Every comparison in a condition, left to right. */
  def comparisons(c: Condition): List[Compare] = c match {
    case compare: Compare => List(compare)
    case And(a, b)        => comparisons(a) ++ comparisons(b)
    case Or(a, b)         => comparisons(a) ++ comparisons(b)
    case Not(a)           => comparisons(a)
  }

  /** Every feature a condition uses. */
  def features(c: Condition): Set[String] = comparisons(c).map(_.feature).toSet

  /** Whether one transaction, given as feature name -> value, matches a condition. */
  def matches(c: Condition, row: Map[String, Any]): Boolean = c match {
    case And(a, b) => matches(a, row) && matches(b, row)
    case Or(a, b)  => matches(a, row) || matches(b, row)
    case Not(a)    => !matches(a, row)
    case Compare(feature, op, Num(v)) =>
      val x = number(feature, valueOf(feature, row))
      op.holds(if (x < v) -1 else if (x > v) 1 else 0)
    case Compare(feature, op, Text(s)) =>
      op.holds(Integer.signum(valueOf(feature, row).toString.compareTo(s)))
  }

  /** The condition as rule text, with brackets that show what binds to what, so it reads back the same. */
  def show(c: Condition): String = c match {
    case Compare(feature, op, Num(v))  => s"$feature ${op.symbol} ${showNumber(v)}"
    case Compare(feature, op, Text(s)) => s"$feature ${op.symbol} " + "\"" + s + "\""
    case And(a, b)                     => s"(${show(a)} and ${show(b)})"
    case Or(a, b)                      => s"(${show(a)} or ${show(b)})"
    case Not(a)                        => s"not ${show(a)}"
  }

  private def valueOf(feature: String, row: Map[String, Any]): Any =
    row.getOrElse(feature, throw new NoSuchElementException(s"no value for feature $feature"))

  private def number(feature: String, value: Any): Double = value match {
    case n: java.lang.Number => n.doubleValue
    case other               => throw new IllegalArgumentException(s"$feature is not a number: $other")
  }

  private def showNumber(v: Double): String =
    if (v.isWhole && math.abs(v) < 1e15) v.toLong.toString else v.toString
}
