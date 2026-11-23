package aml.batch

import aml.common.{FeatureEngine, RawPayment}
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Part 8c: proves the plain-Scala FeatureEngine gives the same features as Spark. It feeds the engine every payment
  * of the week before a day and of the day itself, in time order, exactly as the live monitor would, then compares
  * all its features with Spark's for every payment of that day.
  *
  * Run it from the sbt shell, after GraphFeaturesMain:  batch/runMain aml.batch.FeatureEngineCheckMain [day]
  */
object FeatureEngineCheckMain {

  val DefaultDay = "2022-09-07"
  val Tolerance  = 1e-6   // relative, for sums of US dollars added up in a different order

  def main(args: Array[String]): Unit = {
    val day   = args.headOption.getOrElse(DefaultDay)
    val spark = LocalSpark.session("AML feature engine check")

    try {
      val all      = spark.read.parquet(Stage1Main.GraphFolder)
      val hubs     = GraphFeatures.hubs(all).collect().map(_.getString(0)).toSet
      val usdPer   = spark.read.option("header", "true").csv(IngestMain.RatesFolder).collect()
        .map(r => r.getString(0) -> r.getString(1).toDouble).toMap
      val features = Stage1.numericFeatures(includeHour = true)

      val week = all
        .filter(col("day") >= date_sub(to_date(lit(day)), 7) && col("day") <= to_date(lit(day)))
        .select((Seq(col("txnId"), (unix_timestamp(col("ts")) / 60).cast("long").as("minute"), col("day").cast("string").as("dayText"),
          col("fromBank"), col("fromAcct"), col("toBank"), col("toAcct"), col("amountReceived"), col("recvCcy"),
          col("amountPaid"), col("payCcy"), col("format")) ++ features.map(c => col(c))): _*)
        .orderBy("minute", "txnId")
      println(f"\nFeeding the engine every payment from 7 days before $day to the end of $day, in time order (${hubs.size}%d hubs, ${usdPer.size}%d currencies)")

      // Spark's features for the day being checked, by payment
      val expected = mutable.HashMap.empty[Long, Array[Double]]
      def number(v: Any): Double = v match { case n: java.lang.Number => n.doubleValue; case other => sys.error(s"not a number: $other") }
      val payments = week.toLocalIterator().asScala.map { r =>
        val p = RawPayment(r.getLong(0), r.getLong(1), r.getString(3), r.getString(4), r.getString(5), r.getString(6),
          r.getDouble(7), r.getString(8), r.getDouble(9), r.getString(10), r.getString(11))
        if (r.getString(2) == day) expected(p.id) = features.indices.map(i => number(r.get(12 + i))).toArray
        p
      }

      // The engine's features, compared as they come
      val mismatches = mutable.LinkedHashMap(features.map(_ -> 0L): _*)
      val examples   = mutable.ArrayBuffer.empty[String]
      var fed, compared = 0L
      val started = System.nanoTime()
      FeatureEngine.run(payments, usdPer, hubs).foreach { case (p, f) =>
        fed += 1
        expected.remove(p.id).foreach { want =>
          compared += 1
          features.indices.foreach { i =>
            val got = f(features(i))
            if (math.abs(got - want(i)) > Tolerance * math.max(1.0, math.abs(want(i)))) {
              mismatches(features(i)) += 1
              if (examples.size < 10) examples += f"payment ${p.id}: ${features(i)} is $got%.6f, Spark says ${want(i)}%.6f"
            }
          }
        }
      }
      val seconds = (System.nanoTime() - started) / 1e9

      val total = mismatches.values.sum
      println(f"Fed $fed%,d payments in $seconds%.1f s (${fed / seconds}%,.0f a second, reading from Spark included)")
      println(f"Compared $compared%,d payments of $day, with ${features.size}%d features each: ${compared * features.size}%,d values")
      println(f"Values that differ from Spark's: $total%,d   (expected 0)")
      mismatches.filter(_._2 > 0).foreach { case (n, c) => println(f"  $n%-28s $c%,d") }
      examples.foreach(e => println(s"  for example, $e"))
      if (expected.nonEmpty) println(f"  ${expected.size}%,d payments of $day never came out of the engine (expected 0)")
    } finally spark.stop()
  }
}
