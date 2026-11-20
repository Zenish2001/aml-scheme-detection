package aml.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ScoringModelSpec extends AnyFlatSpec with Matchers {

  // Input 0: amountPaidUsd, input 1: pairPayments7d, input 2: format is ACH, input 3: format is Cheque
  private val model = ScoringModel(
    numeric = Seq("amountPaidUsd", "pairPayments7d"),
    formats = Seq("ACH", "Cheque"),
    trees = Seq(
      WeightedTree(1.0, SplitNode(0, 5000.0, Leaf(-0.5), Leaf(0.8))),
      WeightedTree(0.5, CategoryNode(2, Set(1.0), Leaf(0.4), Leaf(-0.2)))))

  private def sigmoid2(m: Double) = 1.0 / (1.0 + math.exp(-2.0 * m))

  behavior of "ScoringModel"

  it should "lay out the inputs as Spark does: numeric features, then a 1 for the payment format" in {
    model.vector(Map("amountPaidUsd" -> 7500.0, "pairPayments7d" -> 0L, "format" -> "ACH")) shouldBe Array(7500.0, 0.0, 1.0, 0.0)
    model.vector(Map("amountPaidUsd" -> 1, "pairPayments7d" -> 2, "format" -> "Wire")) shouldBe Array(1.0, 2.0, 0.0, 0.0)
  }

  it should "add up the weighted trees and turn the sum into a probability, as Spark's GBT does" in {
    // 7,500 is over 5,000 (0.8), and ACH goes left (0.4 x 0.5): 1.0 in all
    model.score(Map("amountPaidUsd" -> 7500.0, "pairPayments7d" -> 0L, "format" -> "ACH")) shouldBe (sigmoid2(1.0) +- 1e-12)
    // 100 is under 5,000 (-0.5), and Cheque goes right (-0.2 x 0.5): -0.6 in all
    model.score(Map("amountPaidUsd" -> 100.0, "pairPayments7d" -> 0L, "format" -> "Cheque")) shouldBe (sigmoid2(-0.6) +- 1e-12)
  }

  it should "write itself as text and read back exactly the same model" in {
    ScoringModel.fromText(ScoringModel.toText(model)) shouldBe Right(model)
    model.nodeCount shouldBe 6
  }

  it should "say where text goes wrong" in {
    ScoringModel.fromText("numeric a\ntree 1.0\nsplit 0 0.5\nleaf 1.0\n").left.getOrElse("") should include("ends in the middle of a tree")
    ScoringModel.fromText("numeric a\ntree 1.0\nsplit 0 oops\nleaf 1\nleaf 2\n").left.getOrElse("") should include("line 3")
    ScoringModel.fromText("numeric a\ntree 1.0\nsplit 9 0.5\nleaf 1\nleaf 2\n").left.getOrElse("") should include("input 9")
  }
}
