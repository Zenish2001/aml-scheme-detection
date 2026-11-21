package aml.monitor

import java.io.File

/** One payment as the live monitor receives it: who paid whom and when, the truth (known only because this is a
  * replay of labelled data), and its features, as feature name -> value, for the model, rules and reasons.
  */
final case class LivePayment(
  txnId: Long, minute: Long, time: String, sender: String, receiver: String, format: String,
  isLaundering: Boolean, scheme: Option[String], features: Map[String, Any])

/** Reading the replay files that ReplayExportMain writes: CSV with a header, one payment per line. */
object Replay {

  private val Required = Seq("txnId", "minute", "time", "senderAccount", "receiverAccount", "format", "isLaundering", "scheme")
  private val Text     = Set("time", "senderAccount", "receiverAccount", "format", "scheme")

  /** A parser for the lines that follow this header. Every column that isn't text becomes a numeric feature. */
  def parser(header: String): String => LivePayment = {
    val names   = header.split(",", -1).map(_.trim)
    val missing = Required.filterNot(names.contains)
    require(missing.isEmpty, s"the replay file has no ${missing.mkString(", ")} column")
    val at      = names.zipWithIndex.toMap
    val numbers = names.indices.filterNot(i => Text(names(i)))

    line => {
      val v        = line.split(",", -1)
      val features = numbers.map(i => names(i) -> (v(i).toDouble: Any)).toMap + ("format" -> v(at("format")))
      LivePayment(
        v(at("txnId")).toLong, v(at("minute")).toLong, v(at("time")), v(at("senderAccount")), v(at("receiverAccount")),
        v(at("format")), v(at("isLaundering")) == "1", Some(v(at("scheme"))).filter(s => s.nonEmpty && s != "NONE"), features)
    }
  }

  /** The CSV file in a folder Spark wrote, or the path itself if it's a file. */
  def file(path: String): File = {
    val f = new File(path)
    if (f.isFile) f
    else Option(f.listFiles()).toSeq.flatten.find(x => x.getName.startsWith("part-") && x.getName.endsWith(".csv"))
      .getOrElse(throw new IllegalArgumentException(s"no replay file in $path: run batch/runMain aml.batch.ReplayExportMain first"))
  }
}
