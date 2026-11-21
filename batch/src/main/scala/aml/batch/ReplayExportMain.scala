package aml.batch

import aml.common.RuleParser
import java.io.{File, PrintWriter}
import org.apache.spark.sql.functions._

/** Part 7b: prepares everything the live monitor needs, so the monitor itself never starts Spark.
  *
  *  1. A replay file: every payment to stream, in time order, with its 34 features, as CSV. By default the
  *     HI-Small test days; or any dataset TransferMain has prepared, by name (for example LI-Small).
  *  2. data/models/monitor-settings.txt: the alert threshold (the score of the 100th highest payment on the
  *     validation day, so about 100 alerts a day) and the lift of each reason kept (at least 1.5 on the training
  *     days, as in ReasonsMain).
  *
  * Run it from the sbt shell, after ModelExportMain:  batch/runMain aml.batch.ReplayExportMain [dataset]
  */
object ReplayExportMain {

  val SettingsFile = "data/models/monitor-settings.txt"
  val AlertsPerDay = 100

  def main(args: Array[String]): Unit = {
    val dataset = args.headOption.getOrElse("HI-Small")
    val spark   = LocalSpark.session("AML replay export")

    try {
      // 1. The replay file
      val hi       = spark.read.parquet(Stage1Main.GraphFolder).withColumn("period", Stage1.period(col("day")))
      val payments =
        if (dataset == "HI-Small") hi.filter(col("period") === "test")
        else spark.read.parquet(s"data/clean/${dataset.toLowerCase}/features_graph")
      val numeric = Stage1.numericFeatures(includeHour = false)
      val columns = (Seq("txnId", "senderAccount", "receiverAccount", "format", "isLaundering", "scheme") ++ numeric).distinct
      val folder  = s"data/replay/$dataset"
      payments
        .withColumn("minute", (unix_timestamp(col("ts")) / 60).cast("long"))
        .withColumn("time", date_format(col("ts"), "yyyy-MM-dd HH:mm"))
        .na.fill("NONE", Seq("scheme"))
        .select((Seq("minute", "time") ++ columns).map(c => col(c)): _*)
        .repartition(1).sortWithinPartitions("minute", "txnId")
        .write.mode("overwrite").option("header", "true").csv(folder)
      val rows = spark.read.option("header", "true").csv(folder).count()
      println(f"\nWrote $rows%,d payments, in time order, with ${numeric.size}%d features each, to $folder")

      // 2. The alert threshold, from the validation day, so the test days stay unseen
      val threshold = spark.read.parquet(Stage1Main.ScoresFolder).filter(col("period") === "validation")
        .orderBy(desc("score")).limit(AlertsPerDay).agg(min("score")).first().getDouble(0)
      println(f"Alert threshold: $threshold%.6f, the score of the ${AlertsPerDay}th highest payment on the validation day")

      // 3. Each reason's lift on the training days, keeping those of at least MinLift
      val reasons = RuleParser.parseFile(ReasonsMain.ReasonsFile).fold(e => sys.error(s"${ReasonsMain.ReasonsFile}: $e"), identity)
      val train   = hi.filter(col("period") === "train").cache()
      val base    = train.filter(col("isLaundering") === 1).count().toDouble / train.count()
      val kept = reasons.map { reason =>
        val a = Stage1.ofRule(train, RuleColumns.toColumn(reason.condition))
        reason.name -> (if (a.alerts == 0) 0.0 else a.precision / base)
      }.filter { case (_, lift) => lift >= ReasonsMain.MinLift }.sortBy { case (_, lift) => -lift }

      new File(SettingsFile).getParentFile.mkdirs()
      val out = new PrintWriter(SettingsFile, "UTF-8")
      try {
        out.println("# Settings for the live monitor, written by ReplayExportMain")
        out.println(s"threshold $threshold")
        kept.foreach { case (name, lift) => out.println(f"reason $lift%.4f $name") }
      } finally out.close()
      println(s"Kept ${kept.size} of ${reasons.size} reasons, and saved the settings to $SettingsFile")
    } finally spark.stop()
  }
}
