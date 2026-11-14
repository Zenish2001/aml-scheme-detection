package aml.batch

import org.apache.spark.sql.SparkSession

/** One place for how this project starts Spark on a laptop. */
object LocalSpark {

  /** A Spark session using all of this machine's cores.
    *
    * Both driver addresses are 127.0.0.1: on a Mac the hostname can resolve to 127.0.0.1 while
    * Spark advertises the Wi-Fi address, and then Spark can't reach itself.
    */
  def session(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")   // the default of 200 is far too many for one laptop
      .getOrCreate()
}
