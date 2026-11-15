package aml.batch

import java.nio.file.Files
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EllipticSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("EllipticSpec")

  override def afterAll(): Unit = spark.stop()

  /** Writes a tiny file and returns its path. */
  private def file(lines: String*): String = {
    val path = Files.createTempFile("elliptic", ".csv")
    Files.writeString(path, lines.mkString("", "\n", "\n"))
    path.toString
  }

  /** A line like the real features file: txId, time step, then 165 values (0.1, 0.2, ... 16.5). */
  private def featuresLine(txId: Long, timeStep: Int): String =
    (Seq(txId.toString, timeStep.toString) ++ (1 to 165).map(i => (i / 10.0).toString)).mkString(",")

  "Elliptic.readFeatures" should "name all 167 columns: txId, timeStep, 93 local and 72 aggregated features" in {
    val features = Elliptic.readFeatures(spark, file(featuresLine(230425980L, 1)))
    features.columns should have length 167
    features.columns.take(3) shouldBe Array("txId", "timeStep", "local_1")
    features.columns.last shouldBe "agg_72"

    val row = features.collect().head
    row.getAs[Long]("txId") shouldBe 230425980L
    row.getAs[Int]("timeStep") shouldBe 1
    row.getAs[Double]("local_1") shouldBe 0.1
    row.getAs[Double]("agg_72") shouldBe 16.5
    features.filter(Elliptic.isValid).count() shouldBe 1
  }

  "Elliptic.readClasses" should "turn the classes into labels, with isIllicit empty when unknown" in {
    val classes = Elliptic.readClasses(spark, file("txId,class", "1,1", "2,2", "3,unknown"))
    val byId = classes.collect().map { row =>
      row.getAs[Long]("txId") -> ((row.getAs[String]("label"), Option(row.getAs[Integer]("isIllicit")).map(_.intValue)))
    }.toMap

    byId(1L) shouldBe ((Elliptic.Illicit, Some(1)))
    byId(2L) shouldBe ((Elliptic.Licit, Some(0)))
    byId(3L) shouldBe ((Elliptic.Unknown, None))
  }

  "Elliptic.readEdges" should "read each payment as src and dst" in {
    val edges = Elliptic.readEdges(spark, file("txId1,txId2", "1,2", "2,3"))
    edges.columns shouldBe Array("src", "dst")
    edges.collect().map(row => (row.getLong(0), row.getLong(1))).toSet shouldBe Set((1L, 2L), (2L, 3L))
  }

  "Elliptic.transactions" should "label a transaction missing from the classes file as unknown" in {
    val features = Elliptic.readFeatures(spark, file(featuresLine(1L, 1), featuresLine(9L, 1)))
    val classes  = Elliptic.readClasses(spark, file("txId,class", "1,1"))
    val labels = Elliptic.transactions(features, classes).select("txId", "label").collect()
      .map(row => row.getLong(0) -> row.getString(1)).toMap

    labels shouldBe Map(1L -> Elliptic.Illicit, 9L -> Elliptic.Unknown)
  }
}
