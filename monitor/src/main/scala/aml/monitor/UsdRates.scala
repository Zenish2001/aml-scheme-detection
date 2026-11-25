package aml.monitor

import java.io.File
import scala.io.Source
import scala.util.Using

/** The US-dollar value of one unit of each currency, as IngestMain worked them out from the data (data/clean/usd_rates). */
object UsdRates {

  val Folder = "data/clean/usd_rates"

  def load(folder: String = Folder): Either[String, Map[String, Double]] =
    Option(new File(folder).listFiles()).toSeq.flatten.find(f => f.getName.startsWith("part-") && f.getName.endsWith(".csv")) match {
      case None => Left(s"there are no exchange rates in $folder (run IngestMain first)")
      case Some(file) =>
        Using.resource(Source.fromFile(file, "UTF-8")) { source =>
          val rates = source.getLines().drop(1).map(_.split(",")).collect { case Array(c, r) if r.toDoubleOption.isDefined => c -> r.toDouble }.toMap
          if (rates.isEmpty) Left(s"$file has no exchange rates in it") else Right(rates)
        }
    }
}
