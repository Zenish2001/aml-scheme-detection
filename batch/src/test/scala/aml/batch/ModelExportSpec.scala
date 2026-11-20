package aml.batch

import aml.common.ScoringModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ModelExportSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("ModelExportSpec")

  override def afterAll(): Unit = spark.stop()

  "ModelExport" should "give exactly the scores Spark gives, through text and back" in {
    val numeric = Stage1.numericFeatures(includeHour = false)
    val id      = col("id")
    val launder = (id % 10 === 0) || (id % 3 === 0 && id % 7 === 0)
    val data = spark.range(400).select(
      numeric.map {
        case "cyclePaths7d"  => (id % 10 === 0).cast("double").as("cyclePaths7d")
        case "amountPaidUsd" => ((id * 13) % 1000).cast("double").as("amountPaidUsd")
        case n               => lit(0.0).as(n)
      } ++ Seq(when(id % 3 === 0, "ACH").otherwise("Cheque").as("format"), launder.cast("int").as("isLaundering")): _*)

    val sparkModel = Stage1.pipeline(numeric, Stage1.gbt(iterations = 10, depth = 3)).fit(Stage1.withLabel(data))
    val exported   = ScoringModel.fromText(ScoringModel.toText(ModelExport.fromSpark(sparkModel))).fold(e => fail(e), identity)

    val scored = Stage1.score(sparkModel, data)
    val differences = ModelExport.rows(scored, exported, extra = Seq("score"))
      .map(row => math.abs(exported.score(row) - row("score").asInstanceOf[Double]))
    differences.max should be < 1e-9
  }
}
