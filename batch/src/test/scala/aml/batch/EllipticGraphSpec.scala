package aml.batch

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EllipticGraphSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = LocalSpark.session("EllipticGraphSpec")

  override def afterAll(): Unit = spark.stop()

  "EllipticGraph" should "count each transaction's neighbours one and two steps away, with zeros for none" in {
    val session = spark
    import session.implicits._
    val edges = Seq((1L, 2L), (2L, 3L), (2L, 4L)).toDF("src", "dst")   // 1 pays 2, which pays 3 and 4
    val txns  = (1L to 5L).toDF("txId")                                 // 5 has no flows

    val g = EllipticGraph.withGraph(txns, edges).collect()
      .map(r => r.getAs[Long]("txId") -> EllipticGraph.columns.map(c => r.getAs[Long](c))).toMap

    // inDegree, outDegree, in2, out2
    g(1L) shouldBe Seq(0L, 1L, 0L, 2L)
    g(2L) shouldBe Seq(1L, 2L, 0L, 0L)
    g(3L) shouldBe Seq(1L, 0L, 1L, 0L)
    g(5L) shouldBe Seq(0L, 0L, 0L, 0L)
  }
}
