package aml.batch

/** Entry point for the batch path (Spark). Ingestion arrives in Part 1. */
object BatchMain {
  def main(args: Array[String]): Unit =
    println(s"${aml.common.Version.name}: batch path")
}
