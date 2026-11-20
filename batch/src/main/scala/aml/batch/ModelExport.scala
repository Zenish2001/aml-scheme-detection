package aml.batch

import aml.common.{CategoryNode, Leaf, ScoringModel, SplitNode, TreeNode, WeightedTree}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.feature.{StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.tree.{CategoricalSplit, ContinuousSplit, InternalNode, LeafNode, Node}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/** Part 7a: turns a Stage 1 Spark pipeline (payment format indexer, one-hot encoder, vector assembler, GBT) into
  * a ScoringModel, the same trees in plain Scala.
  */
object ModelExport {

  def fromSpark(model: PipelineModel): ScoringModel = {
    val formats = model.stages(0).asInstanceOf[StringIndexerModel].labelsArray(0).toSeq
    val inputs  = model.stages(2).asInstanceOf[VectorAssembler].getInputCols.toSeq
    require(inputs.lastOption.contains("formatVec"), s"expected the payment formats last, found ${inputs.lastOption}")
    val gbt = model.stages(3).asInstanceOf[GBTClassificationModel]
    val trees = gbt.trees.toSeq.zip(gbt.treeWeights.toSeq).map { case (tree, weight) => WeightedTree(weight, convert(tree.rootNode)) }
    ScoringModel(inputs.init, formats, trees)
  }

  private def convert(node: Node): TreeNode = node match {
    case leaf: LeafNode => Leaf(leaf.prediction)
    case inner: InternalNode =>
      inner.split match {
        case s: ContinuousSplit  => SplitNode(s.featureIndex, s.threshold, convert(inner.leftChild), convert(inner.rightChild))
        case s: CategoricalSplit => CategoryNode(s.featureIndex, s.leftCategories.toSet, convert(inner.leftChild), convert(inner.rightChild))
      }
  }

  /** Each row as feature name -> value, with only what the plain-Scala model needs (and `extra` columns). */
  def rows(df: DataFrame, model: ScoringModel, extra: Seq[String] = Nil): Seq[Map[String, Any]] =
    df.select((model.numeric ++ ("format" +: extra)).map(c => col(c)): _*).collect().toSeq
      .map(row => row.getValuesMap[Any](row.schema.fieldNames.toSeq))
}
