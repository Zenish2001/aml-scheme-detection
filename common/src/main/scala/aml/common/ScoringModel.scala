package aml.common

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Using

/** A node of a decision tree. */
sealed trait TreeNode

/** Goes left when the feature is at most the threshold, as in Spark. */
final case class SplitNode(feature: Int, threshold: Double, left: TreeNode, right: TreeNode) extends TreeNode

/** Goes left when the feature's value is one of the left categories (used for the 0/1 payment-format inputs). */
final case class CategoryNode(feature: Int, leftCategories: Set[Double], left: TreeNode, right: TreeNode) extends TreeNode

/** The tree's answer, when a payment reaches this node. */
final case class Leaf(value: Double) extends TreeNode

final case class WeightedTree(weight: Double, root: TreeNode)

/** The Stage 1 model outside Spark: the same gradient-boosted trees, in plain Scala, so the live monitor can score
  * each payment in microseconds without starting Spark. Its inputs are the numeric features, in order, then one
  * 0/1 input per payment format, exactly as Spark's pipeline lays them out.
  */
final case class ScoringModel(numeric: Seq[String], formats: Seq[String], trees: Seq[WeightedTree]) {

  /** The payment as the model's inputs: its numeric features, then a 1 for its payment format (all 0 if unknown). */
  def vector(row: Map[String, Any]): Array[Double] = {
    val x = new Array[Double](numeric.size + formats.size)
    numeric.indices.foreach { i =>
      x(i) = row.getOrElse(numeric(i), throw new NoSuchElementException(s"no value for feature ${numeric(i)}")) match {
        case n: java.lang.Number => n.doubleValue
        case other               => throw new IllegalArgumentException(s"${numeric(i)} is not a number: $other")
      }
    }
    val format = row.get("format").map(String.valueOf).orNull
    formats.indices.foreach(j => if (formats(j) == format) x(numeric.size + j) = 1.0)
    x
  }

  /** The trees' weighted sum. */
  def margin(x: Array[Double]): Double =
    trees.foldLeft(0.0)((sum, tree) => sum + tree.weight * ScoringModel.predict(tree.root, x))

  /** The probability of laundering, from 0 to 1, computed exactly as Spark's GBT model does. */
  def score(row: Map[String, Any]): Double = 1.0 - 1.0 / (1.0 + math.exp(2.0 * margin(vector(row))))

  /** How many nodes the trees have in all. */
  def nodeCount: Int = {
    def count(node: TreeNode): Int = node match {
      case Leaf(_)                     => 1
      case SplitNode(_, _, l, r)       => 1 + count(l) + count(r)
      case CategoryNode(_, _, l, r)    => 1 + count(l) + count(r)
    }
    trees.map(t => count(t.root)).sum
  }
}

/** Writing a model as text and reading it back. The format is one line per item: `numeric <feature>` and
  * `format <payment format>` for the inputs, then for each tree `tree <weight>` and its nodes in order, each node
  * before its left and then its right branch: `split <input> <threshold>`, `category <input> <values>` or
  * `leaf <value>`. Lines starting with # are comments.
  */
object ScoringModel {

  @tailrec
  def predict(node: TreeNode, x: Array[Double]): Double = node match {
    case Leaf(value)                    => value
    case SplitNode(f, threshold, l, r)  => predict(if (x(f) <= threshold) l else r, x)
    case CategoryNode(f, values, l, r)  => predict(if (values.contains(x(f))) l else r, x)
  }

  def toText(model: ScoringModel): String = {
    val b = new StringBuilder("# AML Stage 1 model: gradient-boosted trees, exported from Spark by ModelExportMain\n")
    model.numeric.foreach(f => b ++= s"numeric $f\n")
    model.formats.foreach(f => b ++= s"format $f\n")
    def write(node: TreeNode): Unit = node match {
      case Leaf(value) => b ++= s"leaf $value\n"
      case SplitNode(f, threshold, l, r) =>
        b ++= s"split $f $threshold\n"; write(l); write(r)
      case CategoryNode(f, values, l, r) =>
        val listed = if (values.isEmpty) "-" else values.toSeq.sorted.mkString(",")
        b ++= s"category $f $listed\n"; write(l); write(r)
    }
    model.trees.foreach { t => b ++= s"tree ${t.weight}\n"; write(t.root) }
    b.toString
  }

  private final class Mistake(message: String) extends Exception(message)

  def fromText(text: String): Either[String, ScoringModel] = {
    val lines = text.linesIterator.zipWithIndex
      .map { case (line, i) => (line.trim, i + 1) }
      .filter { case (line, _) => line.nonEmpty && !line.startsWith("#") }
      .toVector
    var pos = 0
    def fail(n: Int, why: String): Nothing = throw new Mistake(s"line $n: $why")
    def number(s: String, n: Int): Double = s.toDoubleOption.getOrElse(fail(n, s"'$s' is not a number"))
    def index(s: String, n: Int): Int = s.toIntOption.getOrElse(fail(n, s"'$s' is not an input number"))

    def node(): TreeNode = {
      if (pos >= lines.size) fail(lines.lastOption.map(_._2).getOrElse(0), "the text ends in the middle of a tree")
      val (line, n) = lines(pos)
      pos += 1
      line.split(" ").toList match {
        case List("leaf", v)        => Leaf(number(v, n))
        case List("split", f, t)    => SplitNode(index(f, n), number(t, n), node(), node())
        case List("category", f, c) =>
          val values = if (c == "-") Set.empty[Double] else c.split(",").map(number(_, n)).toSet
          CategoryNode(index(f, n), values, node(), node())
        case _ => fail(n, s"expected a node (split, category or leaf), found: $line")
      }
    }

    try {
      val numeric = Vector.newBuilder[String]
      val formats = Vector.newBuilder[String]
      val trees   = Vector.newBuilder[WeightedTree]
      while (pos < lines.size) {
        val (line, n) = lines(pos)
        pos += 1
        line.split(" ", 2).toList match {
          case List("numeric", f) => numeric += f
          case List("format", f)  => formats += f
          case List("tree", w)    => trees += WeightedTree(number(w, n), node())
          case _                  => fail(n, s"expected numeric, format or tree, found: $line")
        }
      }
      val model  = ScoringModel(numeric.result(), formats.result(), trees.result())
      val inputs = model.numeric.size + model.formats.size
      def check(node: TreeNode): Unit = node match {
        case Leaf(_) => ()
        case SplitNode(f, _, l, r)    => if (f >= inputs) throw new Mistake(s"a split uses input $f, but there are only $inputs"); check(l); check(r)
        case CategoryNode(f, _, l, r) => if (f >= inputs) throw new Mistake(s"a split uses input $f, but there are only $inputs"); check(l); check(r)
      }
      if (model.trees.isEmpty) Left("the text has no trees")
      else { model.trees.foreach(t => check(t.root)); Right(model) }
    } catch { case m: Mistake => Left(m.getMessage) }
  }

  def load(path: String): Either[String, ScoringModel] =
    Using.resource(Source.fromFile(path))(source => fromText(source.mkString))
}
