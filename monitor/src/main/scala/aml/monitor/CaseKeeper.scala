package aml.monitor

import aml.common.{Cases, Payment}
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

/** An actor that keeps every alert, and groups them into cases on request: alerts that share an account within
  * 3 days, each named by its shape, as in Part 5b. Its state lives only inside the actor, so the stream can send
  * it alerts from any thread without locks.
  */
object CaseKeeper {

  sealed trait Command
  final case class Alerted(decision: Decision) extends Command
  final case class Report(replyTo: ActorRef[Summary]) extends Command

  /** One case: its alerts, and the scheme its shape looks like, with the reason. */
  final case class LiveCase(id: Int, scheme: String, why: String, alerts: Seq[Decision])
  final case class Summary(alerts: Int, cases: Seq[LiveCase])

  val WindowMinutes: Long = 3 * 24 * 60

  def apply(windowMinutes: Long = WindowMinutes): Behavior[Command] = keeping(Vector.empty, windowMinutes)

  private def keeping(alerts: Vector[Decision], window: Long): Behavior[Command] = Behaviors.receiveMessage {
    case Alerted(d) =>
      keeping(alerts :+ d, window)
    case Report(replyTo) =>
      replyTo ! summarize(alerts, window)
      Behaviors.same
  }

  /** The alerts grouped into cases, largest first. */
  def summarize(alerts: Seq[Decision], window: Long = WindowMinutes): Summary = {
    val byId     = alerts.map(d => d.payment.txnId -> d).toMap
    val payments = alerts.map(d => Payment(d.payment.txnId, d.payment.sender, d.payment.receiver, d.payment.minute))
    val cases = Cases.group(payments, window).map { ps =>
      val shape = Cases.shapeOf(ps)
      (shape, ps.map(p => byId(p.id)))
    }.sortBy { case (_, ds) => -ds.size }.zipWithIndex.map { case ((shape, ds), i) => LiveCase(i + 1, shape.scheme, shape.why, ds) }
    Summary(alerts.size, cases)
  }
}
