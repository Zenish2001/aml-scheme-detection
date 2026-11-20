package aml.batch

import aml.common.ScoringModel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._

/** Part 7a: exports the Stage 1 model's trees to a text file, reads them back, checks the plain-Scala model gives
  * exactly Spark's scores on a sample of the test days, and measures how fast it scores one payment.
  *
  * Run it from the sbt shell, after Stage1Main:  batch/runMain aml.batch.ModelExportMain
  */
object ModelExportMain {

  val TreesFile      = "data/models/stage1-trees.txt"
  val SampleFraction = 0.01   // about 18,000 test-day payments
  val Tolerance      = 1e-9

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML model export")

    try {
      // 1. Export, and read back
      val sparkModel = PipelineModel.load(Stage1Main.ModelFolder)
      val exported   = ModelExport.fromSpark(sparkModel)
      Files.write(Paths.get(TreesFile), ScoringModel.toText(exported).getBytes(UTF_8))
      val model = ScoringModel.load(TreesFile).fold(e => sys.error(s"$TreesFile can't be read back: $e"), identity)
      val kb    = Files.size(Paths.get(TreesFile)) / 1024
      println(f"\nExported ${model.trees.size}%d trees (${model.nodeCount}%,d nodes), ${model.numeric.size}%d numeric inputs and " +
        f"${model.formats.size}%d payment formats to $TreesFile ($kb%,d KB)")
      println(s"Read back exactly the same model: ${model == exported}")

      // 2. The same scores as Spark, on a sample of the test days
      val sample = spark.read.parquet(Stage1Main.GraphFolder)
        .withColumn("period", Stage1.period(col("day")))
        .filter(col("period") === "test")
        .sample(withReplacement = false, fraction = SampleFraction, seed = 42)
      val rows        = ModelExport.rows(Stage1.score(sparkModel, sample), model, extra = Seq("score", "isLaundering"))
      val differences = rows.map(row => math.abs(model.score(row) - row("score").asInstanceOf[Double]))
      println(f"\nCompared on ${rows.size}%,d test-day payments (${rows.count(_("isLaundering") == 1)}%,d of them laundering):")
      println(f"  largest difference from Spark's score: ${differences.max}%.2e")
      println(f"  payments differing by more than $Tolerance%.0e: ${differences.count(_ > Tolerance)}%,d   (expected 0)")

      // 3. Speed: score every sampled payment several times, timing each one on the last round
      val inputs = rows.map(_ -- Seq("score", "isLaundering"))
      (1 to 5).foreach(_ => inputs.foreach(model.score))   // warm up, so the JVM compiles the scoring code
      val nanos = inputs.map { row =>
        val start = System.nanoTime()
        model.score(row)
        System.nanoTime() - start
      }.sorted
      def micros(q: Double) = nanos(math.min(nanos.size - 1, (q * nanos.size).toInt)) / 1000.0
      println(f"\nTime to score one payment, without Spark: median ${micros(0.5)}%.1f microseconds, " +
        f"95th percentile ${micros(0.95)}%.1f, 99th ${micros(0.99)}%.1f")
      println(f"That's about ${1e6 / micros(0.5)}%,.0f payments a second on one core.")
    } finally spark.stop()
  }
}
