package aml.monitor

import aml.common.{Rule, Rules}
import akka.Done
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

/** Part 8b: the rule-engine actor. It applies the hand-written rules in rules/aml.rules to every payment, live,
  * beside the model, and counts what each rule flags, how much of that was real laundering, and how much the
  * model flagged too. The stream sends payments in batches and waits for each batch to be checked, so the actor
  * can never fall behind and fill up memory (backpressure).
  */
object RuleEngine {

  sealed trait Command
  final case class CheckBatch(decisions: Seq[Decision], replyTo: ActorRef[Done], run: Long = 0) extends Command
  final case class Report(replyTo: ActorRef[Stats]) extends Command
  /** Starts the counts again for a new run (a new file); batches still arriving from older runs are ignored. */
  final case class Reset(run: Long) extends Command

  final case class RuleStat(name: String, alerts: Long, real: Long, alsoModel: Long)
  final case class Stats(payments: Long, laundering: Long, modelAlerts: Long, modelReal: Long, rules: Seq[RuleStat])

  def empty(rules: Seq[Rule]): Stats = Stats(0, 0, 0, 0, rules.map(r => RuleStat(r.name, 0, 0, 0)))

  /** The counts after one more payment. Pure, so it can be tested without an actor. */
  def check(rules: Seq[Rule], s: Stats, d: Decision): Stats = {
    val real = if (d.payment.isLaundering) 1L else 0L
    val both = if (d.isAlert) 1L else 0L
    Stats(s.payments + 1, s.laundering + real, s.modelAlerts + both, s.modelReal + both * real,
      rules.zip(s.rules).map { case (rule, st) =>
        if (Rules.matches(rule.condition, d.payment.features))
          st.copy(alerts = st.alerts + 1, real = st.real + real, alsoModel = st.alsoModel + both)
        else st
      })
  }

  def apply(rules: Seq[Rule]): Behavior[Command] = running(rules, empty(rules), 0)

  private def running(rules: Seq[Rule], s: Stats, run: Long): Behavior[Command] = Behaviors.receiveMessage {
    case CheckBatch(decisions, replyTo, batchRun) =>
      replyTo ! Done
      if (batchRun == run) running(rules, decisions.foldLeft(s)((acc, d) => check(rules, acc, d)), run)
      else Behaviors.same
    case Report(replyTo) =>
      replyTo ! s
      Behaviors.same
    case Reset(newRun) =>
      running(rules, empty(rules), newRun)
  }
}
