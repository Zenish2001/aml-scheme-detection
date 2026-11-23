package aml.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Part 8a: analyst time. An analyst spends about MinutesPerAlert on each alert, so the fewer alerts it takes to
  * find the same laundering, the fewer hours a bank spends. This finds how many of the model's highest-scored
  * payments it takes to catch as much laundering as a rule does.
  */
object AnalystHours {

  val MinutesPerAlert = 20

  def hours(alerts: Long): Double = alerts * MinutesPerAlert / 60.0

  /** How many of the highest-scored payments it takes to include `target` laundering ones, or every payment if
    * even all of them don't. Needs score and isLaundering.
    */
  def alertsToCatch(scored: DataFrame, target: Long): Long =
    if (target <= 0) 0L
    else {
      val ranked = scored.select(col("score"), col("isLaundering"))
        .withColumn("rank", row_number().over(Window.orderBy(desc("score"))))
        .withColumn("caught", sum("isLaundering").over(Window.orderBy("rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      val first = ranked.filter(col("caught") >= target).agg(min("rank")).first()
      if (first.isNullAt(0)) scored.count() else first.getInt(0).toLong
    }
}
