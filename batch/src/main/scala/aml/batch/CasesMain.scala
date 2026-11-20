package aml.batch

import aml.common.{Cases, Payment}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

/** One laundering payment, with its case and the case's shape, as saved for the live monitor. */
final case class CasePayment(txnId: Long, caseId: Int, caseScheme: String, caseWhy: String, scheme: String, attemptId: Option[Int])

/** Part 5b: names the scheme of whole cases, from their shape, instead of single payments.
  *
  *  1. IBM's own attempts as cases: if a case is put together perfectly, does its shape name the scheme?
  *  2. Cases put together without IBM's help, from laundering payments that share an account within a time
  *     window. Several windows are tried, and one is chosen on Sep 1-6, so the test days stay unseen.
  *  3. The laundering IBM didn't list: what shapes do its cases have?
  *
  * Run it from the sbt shell:  batch/runMain aml.batch.CasesMain
  */
object CasesMain {

  val CasesFolder = "data/clean/cases"

  /** The windows tried for linking payments that share an account. */
  val Windows: Seq[(String, Long)] =
    Seq("1 day" -> 1440L, "3 days" -> 4320L, "7 days" -> 10080L, "14 days" -> 20160L, "no limit" -> Long.MaxValue)

  private val short = Map(
    "FAN-OUT" -> "F-OUT", "FAN-IN" -> "F-IN", "CYCLE" -> "CYCLE", "SCATTER-GATHER" -> "S-G",
    "GATHER-SCATTER" -> "G-S", "STACK" -> "STACK", "BIPARTITE" -> "BIPART", "RANDOM" -> "RANDOM")

  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("AML cases")
    import spark.implicits._

    try {
      // 1. Every laundering payment, with the IBM attempt it belongs to, if it's listed
      val laundering = spark.read.parquet(Stage1Main.GraphFolder)
        .filter(col("isLaundering") === 1)
        .withColumn("period", Stage1.period(col("day")))
        .withColumn("minute", (unix_timestamp(col("ts")) / 60).cast("long"))
      val attempts = Labels.readAttempts(IngestMain.PatternsFile).toIndexedSeq
      val attemptRows = attempts.zipWithIndex.flatMap { case (a, i) => a.lines.map(line => (i, line)) }
        .toDF("attemptId", "line")
        .withColumn("t", from_csv(col("line"), Ingest.txnSchema, Map.empty[String, String]))
        .select((col("attemptId") +: Labels.JoinKeys.map(k => col(s"t.$k"))): _*)
      val attemptOf = laundering.join(attemptRows, Labels.JoinKeys).groupBy("txnId").agg(min("attemptId").as("attemptId"))
      val rows = laundering.join(attemptOf, Seq("txnId"), "left")
        .select("txnId", "senderAccount", "receiverAccount", "minute", "scheme", "period", "attemptId")
        .collect().toSeq

      val payments  = rows.map(r => Payment(r.getAs[Long]("txnId"), r.getAs[String]("senderAccount"), r.getAs[String]("receiverAccount"), r.getAs[Long]("minute")))
      val schemeOf  = rows.map(r => r.getAs[Long]("txnId") -> r.getAs[String]("scheme")).toMap
      val periodOf  = rows.map(r => r.getAs[Long]("txnId") -> r.getAs[String]("period")).toMap
      val attemptIn = rows.flatMap(r => attemptIdOf(r).map(r.getAs[Long]("txnId") -> _)).toMap
      println(f"\n${payments.size}%,d laundering payments; ${attemptIn.size}%,d of them in IBM's ${attempts.size}%d listed attempts")

      val listedPayments = payments.filter(p => attemptIn.contains(p.id))
      val choosingDays   = listedPayments.filter(p => Set("train", "validation")(periodOf(p.id)))
      val testDays       = listedPayments.filter(p => periodOf(p.id) == "test")

      // 2. IBM's attempts as cases
      val byAttempt     = listedPayments.groupBy(p => attemptIn(p.id)).toSeq
      val attemptShapes = byAttempt.map { case (id, ps) => (id, attempts(id).scheme, Cases.shapeOf(ps), ps.size) }
      report(s"1. IBM's ${attemptShapes.size} attempts as cases, each attempt counted once",
        tally(attemptShapes.map { case (_, truth, shape, _) => (truth, shape.scheme) }))

      val shapeOfAttempt = attemptShapes.map { case (id, _, shape, _) => id -> shape.scheme }.toMap
      val fromAttempts   = SchemeMetrics.of(tally(testDays.map(p => (schemeOf(p.id), shapeOfAttempt(attemptIn(p.id))))))
      println(f"Each test-day payment named by its attempt's shape: accuracy ${fromAttempts.accuracy * 100}%.1f%%, " +
        f"macro F1 ${fromAttempts.macroF1}%.3f (the Stage 2 model: 34.5%%, 0.315)")

      val wrong = attemptShapes.filter { case (_, truth, shape, _) => truth != shape.scheme }
      val tiny  = wrong.count { case (_, _, _, size) => size <= 3 }
      println(f"${wrong.size}%d attempts are named wrongly, and $tiny%d of them have 3 payments or fewer, too few to show a shape. Some:")
      wrong.sortBy { case (_, _, _, size) => -size }.take(4).foreach { case (id, truth, shape, size) =>
        println(s"  attempt $id, really $truth ($size payments): looks like ${shape.scheme}, because ${shape.why}")
      }

      // 3. Cases put together without IBM's attempts, for several windows
      println("\n2. Cases put together from laundering payments that share an account within a window.")
      println("Whole: attempts kept whole in one case. Pure: cases with listed payments made of exactly one attempt.")
      println("Accuracy: listed payments named right by their case's shape, on Sep 1-6 (for choosing) and the test days.")
      println("%-10s %8s %8s %8s %14s %14s".format("window", "cases", "whole", "pure", "Sep 1-6 acc.", "test acc."))
      val tried = Windows.map { case (name, window) =>
        val cases     = Cases.group(payments, window)
        val shapes    = cases.map(Cases.shapeOf)
        val caseOf    = cases.zipWithIndex.flatMap { case (ps, i) => ps.map(_.id -> i) }.toMap
        val shapeOfP  = cases.zip(shapes).flatMap { case (ps, shape) => ps.map(_.id -> shape.scheme) }.toMap
        val whole     = byAttempt.count { case (_, ps) => ps.map(p => caseOf(p.id)).distinct.size == 1 }.toDouble / byAttempt.size
        val withListd = cases.filter(_.exists(p => attemptIn.contains(p.id)))
        val pure      = withListd.count(ps => ps.forall(p => attemptIn.contains(p.id)) && ps.map(p => attemptIn(p.id)).distinct.size == 1)
          .toDouble / withListd.size
        val choosing  = SchemeMetrics.of(tally(choosingDays.map(p => (schemeOf(p.id), shapeOfP(p.id)))))
        val test      = SchemeMetrics.of(tally(testDays.map(p => (schemeOf(p.id), shapeOfP(p.id)))))
        println("%-10s %,8d %7.1f%% %7.1f%% %13.1f%% %13.1f%%".format(
          name, cases.size, whole * 100, pure * 100, choosing.accuracy * 100, test.accuracy * 100))
        (name, cases, shapes, choosing.accuracy)
      }
      val (chosen, cases, shapes, _) = tried.maxBy(_._4)
      println(s"Chosen on Sep 1-6: $chosen")

      val withTruth = cases.zip(shapes).flatMap { case (ps, shape) =>
        val listed = ps.filter(p => attemptIn.contains(p.id)).map(p => schemeOf(p.id))
        if (listed.isEmpty) None else Some((shape, listed.groupBy(identity).maxBy(_._2.size)._1))
      }
      report(s"With a window of $chosen, each case with listed payments, named by its shape, against its commonest listed scheme",
        tally(withTruth.map { case (shape, truth) => (truth, shape.scheme) }))
      val shapeOfPayment = cases.zip(shapes).flatMap { case (ps, shape) => ps.map(_.id -> shape.scheme) }.toMap
      val fromCases      = SchemeMetrics.of(tally(testDays.map(p => (schemeOf(p.id), shapeOfPayment(p.id)))))
      println(f"Each test-day payment named by its case's shape: accuracy ${fromCases.accuracy * 100}%.1f%%, macro F1 ${fromCases.macroF1}%.3f")

      // 4. The laundering IBM didn't list
      val unlisted = cases.zip(shapes).filter { case (ps, _) => ps.forall(p => !attemptIn.contains(p.id)) }
      println(f"\n3. With a window of $chosen, the ${unlisted.size}%,d cases made only of unlisted laundering, by shape:")
      unlisted.groupBy(_._2.scheme).toSeq.sortBy(-_._2.size).foreach { case (s, cs) =>
        println("  %-16s %,6d cases, %,6d payments".format(s, cs.size, cs.map(_._1.size).sum))
      }

      // 5. Save every laundering payment with its case and the case's shape
      val saved = cases.zip(shapes).zipWithIndex.flatMap { case ((ps, shape), caseId) =>
        ps.map(p => CasePayment(p.id, caseId, shape.scheme, shape.why, schemeOf(p.id), attemptIn.get(p.id)))
      }
      saved.toDS().write.mode("overwrite").parquet(CasesFolder)
      println(f"\nSaved ${saved.size}%,d payments with their cases to $CasesFolder")
    } finally spark.stop()
  }

  private def attemptIdOf(r: Row): Option[Int] =
    if (r.isNullAt(r.fieldIndex("attemptId"))) None else Some(r.getAs[Int]("attemptId"))

  private def tally(pairs: Seq[(String, String)]): Map[(String, String), Long] =
    pairs.groupBy(identity).map { case (pair, xs) => pair -> xs.size.toLong }

  /** Accuracy, macro F1, each scheme's results and the confusion between schemes. */
  private def report(title: String, confusion: Map[(String, String), Long]): Unit = {
    val m = SchemeMetrics.of(confusion)
    println(f"\n$title: accuracy ${m.accuracy * 100}%.1f%%, macro F1 ${m.macroF1}%.3f")
    println("%-16s %9s %11s %9s %7s".format("scheme", "support", "precision", "recall", "F1"))
    m.perScheme.sortBy(-_.support).foreach { s =>
      println("%-16s %,9d %10.1f%% %8.1f%% %7.3f".format(s.scheme, s.support, s.precision * 100, s.recall * 100, s.f1))
    }
    println("Rows are the true scheme, columns the shape:")
    println("%-16s".format("") + Stage2.Schemes.map(s => "%8s".format(short(s))).mkString)
    Stage2.Schemes.foreach { truth =>
      println("%-16s".format(truth) + Stage2.Schemes.map(guess => "%8d".format(confusion.getOrElse((truth, guess), 0L))).mkString)
    }
  }
}
