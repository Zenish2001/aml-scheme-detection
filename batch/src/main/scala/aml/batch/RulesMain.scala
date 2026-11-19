package aml.batch

import aml.common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{NumericType, StringType, StructType}

/** Part 4b: reads the rules in rules/aml.rules (or a file given as an argument), checks them against the data,
  * and measures each one on the test days, against the Stage 1 model given the same number of alerts.
  *
  * Run it from the sbt shell, after Stage1Main:  batch/runMain aml.batch.RulesMain
  */
object RulesMain {

  val RulesFile = "rules/aml.rules"

  def main(args: Array[String]): Unit = {
    val path = args.headOption.getOrElse(RulesFile)
    RuleParser.parseFile(path) match {
      case Left(error) => println(s"\n$path has a mistake, at $error")
      case Right(rules) if rules.isEmpty => println(s"\n$path has no rules")
      case Right(rules) => measure(path, rules)
    }
  }

  private def measure(path: String, rules: List[Rule]): Unit = {
    val spark = LocalSpark.session("AML rules")

    try {
      val all      = spark.read.parquet(Stage1Main.GraphFolder).withColumn("period", Stage1.period(col("day")))
      val problems = for {
        rule    <- rules
        compare <- Rules.comparisons(rule.condition)
        problem <- problemWith(all.schema, compare)
      } yield s"${rule.name}: $problem"

      if (problems.nonEmpty) {
        println(s"\nThe rules in $path don't fit the data:")
        problems.foreach(p => println(s"  $p"))
      } else {
        val test   = all.filter(col("period") === "test").cache()
        val scores = spark.read.parquet(Stage1Main.ScoresFolder).filter(col("period") === "test").select("txnId", "score")
        val scored = test.join(scores, Seq("txnId")).cache()
        val days   = scored.select("day").distinct().count()

        println(s"\n${rules.size} rules read from $path, every feature checked against the data.")
        println("How each rule was read (the brackets show what binds to what):")
        rules.foreach(rule => println(s"  ${rule.name}:  ${Rules.show(rule.condition)}"))

        println("\nOn the test days (Sep 7-10), each rule, and the Stage 1 model given as many alerts:")
        println("%-46s %10s %9s %12s %12s".format("rule", "alerts/day", "recall", "precision", "model recall"))
        val named = rules.map(rule => (rule.name, RuleColumns.toColumn(rule.condition))) :+
          ("Any of the rules above" -> rules.map(rule => RuleColumns.toColumn(rule.condition)).reduce(_ || _))
        named.foreach { case (name, column) =>
          val r = Stage1.ofRule(test, column)
          val m = Stage1.topN(scored, r.alerts)
          println("%-46s %,10d %8.1f%% %11.2f%% %11.1f%%".format(
            shorten(name), r.alerts / days, r.recall * 100, r.precision * 100, m.recall * 100))
        }
      }
    } finally spark.stop()
  }

  /** What's wrong with a comparison, if anything: a feature that doesn't exist, or a value of the wrong kind. */
  private def problemWith(schema: StructType, compare: Compare): Option[String] =
    schema.find(_.name == compare.feature) match {
      case None => Some(s"there's no feature called ${compare.feature}")
      case Some(field) =>
        (field.dataType, compare.value) match {
          case (_: NumericType, Num(_)) | (_: StringType, Text(_)) => None
          case (dataType, value) => Some(s"${compare.feature} holds ${dataType.simpleString}, which can't be compared with $value")
        }
    }

  private def shorten(name: String): String = if (name.length > 46) name.take(45) + "\u2026" else name
}
