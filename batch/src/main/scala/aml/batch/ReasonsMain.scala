package aml.batch

import aml.common._
import java.sql.{Date, Timestamp}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** One reason for an alert: its text, and its lift on the training days. */
final case class ExplainedReason(text: String, lift: Double)

/** One alert, with its reasons, strongest first, as saved for the live monitor and the dashboard. */
final case class ExplainedAlert(
  txnId: Long, day: Date, ts: Timestamp, senderAccount: String, receiverAccount: String, amountPaidUsd: Double,
  format: String, score: Double, rank: Int, isLaundering: Int, scheme: String, reasons: Seq[ExplainedReason])

/** Part 4c: a reason for every alert. Reads the reasons in rules/reasons.rules, measures how strongly each pointed
  * to laundering on the training days (its lift), keeps those with a lift of at least MinLift, explains the top
  * 100 alerts of each test day, checks how well the reasons cover the alerts, prints some in full, and saves them.
  *
  * Run it from the sbt shell, after Stage1Main:  batch/runMain aml.batch.ReasonsMain
  */
object ReasonsMain {

  val ReasonsFile  = "rules/reasons.rules"
  val AlertsFolder = "data/clean/alerts"
  val JsonFolder   = "data/clean/alerts_json"
  val AlertsPerDay = 100
  val MinLift      = 1.5   // a reason must make laundering at least 1.5 times as likely on its own, or it misleads

  def main(args: Array[String]): Unit = RuleParser.parseFile(ReasonsFile) match {
    case Left(error)                     => println(s"\n$ReasonsFile has a mistake, at $error")
    case Right(reasons) if reasons.isEmpty => println(s"\n$ReasonsFile has no reasons")
    case Right(reasons)                  => explainAlerts(reasons)
  }

  private def explainAlerts(reasons: List[Rule]): Unit = {
    val spark = LocalSpark.session("AML reasons")
    import spark.implicits._

    try {
      val all = spark.read.parquet(Stage1Main.GraphFolder).withColumn("period", Stage1.period(col("day")))
      val columns  = all.columns.toSet
      val problems = reasons.flatMap { reason =>
        Rules.comparisons(reason.condition).flatMap(c => RulesMain.problemWith(all.schema, c)) ++
          Reasons.placeholders(reason.name).filterNot(columns).map(f => s"the text refers to {$f}, which isn't a feature")
      }
      if (problems.nonEmpty) {
        println(s"\nThe reasons in $ReasonsFile don't fit the data:")
        problems.foreach(p => println(s"  $p"))
      } else {
        // 1. How strongly each reason pointed to laundering on the training days: its lift
        val train           = all.filter(col("period") === "train").cache()
        val trainRows       = train.count()
        val trainLaundering = train.filter(col("isLaundering") === 1).count()
        val baseRate        = trainLaundering.toDouble / trainRows
        val strengths = reasons.map { reason =>
          val a = Stage1.ofRule(train, RuleColumns.toColumn(reason.condition))
          (reason, a.alerts.toDouble / trainRows, if (a.alerts == 0) 0.0 else a.precision / baseRate)
        }.sortBy { case (_, _, lift) => -lift }
        val (used, unused) = strengths.partition { case (_, _, lift) => lift >= MinLift }
        val ordered = used.map(_._1)
        val liftOf  = strengths.map { case (reason, _, lift) => reason -> lift }.toMap

        println(f"\n${reasons.size}%d reasons, strongest first. Lift: how many times more common laundering is among the")
        println(f"transactions a reason matches than among all of them, on the training days (Sep 1-5):")
        println("%-78s %9s %8s".format("reason", "matches", "lift"))
        used.foreach { case (reason, share, lift) =>
          println("%-78s %8.2f%% %7.1fx".format(shorten(reason.name, 78), share * 100, lift))
        }
        println(f"Not used, because on their own they don't make laundering at least $MinLift%.1f times as likely:")
        unused.foreach { case (reason, share, lift) =>
          println("%-78s %8.2f%% %7.1fx".format(shorten(reason.name, 78), share * 100, lift))
        }

        // 2. The alerts: the top AlertsPerDay of each test day, explained in plain Scala, as the live monitor will
        val scores = spark.read.parquet(Stage1Main.ScoresFolder).filter(col("period") === "test").select("txnId", "score")
        val needed = ordered.flatMap(r => Rules.features(r.condition) ++ Reasons.placeholders(r.name)).distinct
        val basics = Seq("txnId", "day", "ts", "senderAccount", "receiverAccount", "amountPaidUsd", "format", "score",
                         "rank", "isLaundering", "scheme")
        val rows = all.filter(col("period") === "test").join(scores, Seq("txnId"))
          .withColumn("rank", row_number().over(Window.partitionBy("day").orderBy(desc("score"))))
          .filter(col("rank") <= AlertsPerDay)
          .select((basics ++ needed).distinct.map(col): _*)
          .orderBy("day", "rank")
          .collect()

        val alerts = rows.toSeq.map { row =>
          val values = row.getValuesMap[Any](row.schema.fieldNames.toSeq)
          ExplainedAlert(
            row.getAs[Long]("txnId"), row.getAs[Date]("day"), row.getAs[Timestamp]("ts"),
            row.getAs[String]("senderAccount"), row.getAs[String]("receiverAccount"), row.getAs[Double]("amountPaidUsd"),
            row.getAs[String]("format"), row.getAs[Double]("score"), row.getAs[Int]("rank"),
            row.getAs[Int]("isLaundering"), row.getAs[String]("scheme"),
            ordered.filter(reason => Rules.matches(reason.condition, values))
              .map(reason => ExplainedReason(Reasons.render(reason.name, values), liftOf(reason))))
        }

        // 3. How well the reasons cover the alerts
        val caught = alerts.filter(_.isLaundering == 1)
        val wrong  = alerts.filter(_.isLaundering == 0)
        def share(group: Seq[ExplainedAlert], test: ExplainedAlert => Boolean): Double =
          if (group.isEmpty) 0.0 else group.count(test) * 100.0 / group.size
        def average(group: Seq[ExplainedAlert]): Double =
          if (group.isEmpty) 0.0 else group.map(_.reasons.size).sum.toDouble / group.size

        println(s"\nThe top $AlertsPerDay alerts of each test day: ${alerts.size} alerts, ${caught.size} of them laundering")
        println("%-22s %10s %16s %16s %14s".format("", "alerts", "1+ reasons", "2+ reasons", "avg reasons"))
        Seq("All alerts" -> alerts, "Caught laundering" -> caught, "False alarms" -> wrong).foreach { case (name, group) =>
          println("%-22s %,10d %15.1f%% %15.1f%% %14.1f".format(
            name, group.size, share(group, _.reasons.nonEmpty), share(group, _.reasons.size >= 2), average(group)))
        }

        // 4. Each reason among the alerts
        println("\nHow often each reason explains an alert, and how often those alerts are real laundering:")
        println("%-78s %8s %12s".format("reason", "alerts", "laundering"))
        val valuesByAlert = rows.toSeq.map(row => row.getValuesMap[Any](row.schema.fieldNames.toSeq))
        ordered.foreach { reason =>
          val matched = alerts.zip(valuesByAlert).collect { case (alert, values) if Rules.matches(reason.condition, values) => alert }
          println("%-78s %,8d %11.1f%%".format(shorten(reason.name, 78), matched.size, share(matched, _.isLaundering == 1)))
        }

        // 5. Some alerts in full: the three highest-ranked laundering ones, and the two highest-ranked false alarms
        println("\nSome alerts in full:")
        (caught.take(3) ++ wrong.take(2)).foreach { a =>
          val scheme = Option(a.scheme).getOrElse("no scheme")
          val truth  = if (a.isLaundering == 1) s"laundering, $scheme" else "a false alarm"
          println(f"\n  #${a.rank}%d on ${a.day}, ${a.ts}: ${a.senderAccount} paid ${a.receiverAccount} " +
            f"$$${a.amountPaidUsd}%,.0f by ${a.format}. Score ${a.score}%.3f. Really: $truth")
          if (a.reasons.isEmpty) println("    (no reason matched)")
          a.reasons.foreach(r => println(f"    - ${r.text} (lift ${r.lift}%.1fx)"))
        }

        // 6. Save: Parquet for the pipeline, and JSON for the monitor and the dashboard
        val saved = alerts.toDS()
        saved.write.mode("overwrite").parquet(AlertsFolder)
        saved.coalesce(1).write.mode("overwrite").json(JsonFolder)
        println(s"\nSaved ${alerts.size} alerts with their reasons to $AlertsFolder, and as JSON to $JsonFolder")
      }
    } finally spark.stop()
  }

  private def shorten(text: String, width: Int): String = if (text.length > width) text.take(width - 1) + "\u2026" else text
}
