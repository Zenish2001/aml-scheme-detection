package aml.monitor

/** Entry point for the real-time monitor (Akka). The actors arrive in Part 7. */
object MonitorMain {
  def main(args: Array[String]): Unit =
    println(s"${aml.common.Version.name}: live monitor")
}
