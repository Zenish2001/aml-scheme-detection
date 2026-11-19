package aml.batch

import aml.common._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/** Turns a rule's condition into a Spark column, so a rule can filter millions of transactions at once.
  * The same tree is evaluated in plain Scala by Rules.matches; RuleColumnsSpec checks they always agree.
  */
object RuleColumns {

  def toColumn(c: Condition): Column = c match {
    case Compare(feature, op, value) =>
      val left  = col(feature)
      val right = value match {
        case Num(v)  => lit(v)
        case Text(s) => lit(s)
      }
      op match {
        case Op.Eq => left === right
        case Op.Ne => left =!= right
        case Op.Gt => left > right
        case Op.Ge => left >= right
        case Op.Lt => left < right
        case Op.Le => left <= right
      }
    case And(a, b) => toColumn(a) && toColumn(b)
    case Or(a, b)  => toColumn(a) || toColumn(b)
    case Not(a)    => !toColumn(a)
  }
}
