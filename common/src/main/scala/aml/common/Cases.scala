package aml.common

import scala.collection.mutable

/** One payment in a case: who paid whom, and when, in minutes since 1970. */
final case class Payment(id: Long, from: String, to: String, minute: Long)

/** A case's shape: the scheme it looks like, and why, in words an analyst can check. */
final case class Shape(scheme: String, why: String)

/** Cases: groups of connected suspicious payments, named by their shape. Plain Scala, with no Spark, so the live
  * monitor (Part 7) can build and name cases too.
  */
object Cases {

  /** Groups payments into cases. Two payments are in the same case when they share an account and happen within
    * `windowMinutes` of each other, directly or through a chain of such links. Each case comes in time order.
    */
  def group(payments: Seq[Payment], windowMinutes: Long): Seq[Seq[Payment]] = {
    val parent = Array.tabulate(payments.size)(identity)
    def root(i: Int): Int = if (parent(i) == i) i else { parent(i) = root(parent(i)); parent(i) }
    def join(a: Int, b: Int): Unit = { val (ra, rb) = (root(a), root(b)); if (ra != rb) parent(ra) = rb }

    // For each account, link each of its payments to its next one, when they're close enough in time
    val byAccount = payments.indices.flatMap(i => Seq(payments(i).from -> i, payments(i).to -> i)).groupBy(_._1)
    byAccount.values.foreach { pairs =>
      pairs.map(_._2).distinct.sortBy(i => payments(i).minute).sliding(2).foreach {
        case Seq(a, b) if payments(b).minute - payments(a).minute <= windowMinutes => join(a, b)
        case _                                                                    => ()
      }
    }
    payments.indices.groupBy(root).values.map(_.map(payments).sortBy(_.minute)).toSeq.sortBy(_.head.minute)
  }

  /** The scheme a case's payments look like. Repeated payments between the same two accounts count once,
    * and payments to oneself are ignored. The checks run from the most specific shape to the least.
    */
  def shapeOf(payments: Seq[Payment]): Shape = {
    val edges   = payments.filter(p => p.from != p.to).map(p => (p.from, p.to)).distinct
    val nodes   = (edges.map(_._1) ++ edges.map(_._2)).distinct
    val out     = edges.groupBy(_._1).map { case (n, es) => n -> es.size }.withDefaultValue(0)
    val in      = edges.groupBy(_._2).map { case (n, es) => n -> es.size }.withDefaultValue(0)
    val sources = nodes.filter(n => in(n) == 0)                    // only pay
    val sinks   = nodes.filter(n => out(n) == 0)                   // only receive
    val middles = nodes.filter(n => in(n) > 0 && out(n) > 0)       // receive and pass on
    val order   = topologicalOrder(nodes, edges)                   // None when the money goes round a loop
    val n       = edges.size

    def onlyBetween(test: ((String, String)) => Boolean): Boolean = edges.forall(test)

    if (n == 0)
      Shape("RANDOM", "no payments between different accounts")
    else if (sources.isEmpty)
      Shape("CYCLE", s"the money goes round ${nodes.size} accounts and back to where it started")
    else if (n >= 2 && sources.size == 1 && out(sources.head) == n)
      Shape("FAN-OUT", s"one account paid $n others")
    else if (n >= 2 && sinks.size == 1 && in(sinks.head) == n)
      Shape("FAN-IN", s"$n accounts paid one")
    else if (sources.size == 1 && sinks.size == 1 && middles.size >= 2 && middles.forall(m => in(m) == 1 && out(m) == 1) &&
             onlyBetween { case (a, b) => (a == sources.head && middles.contains(b)) || (middles.contains(a) && b == sinks.head) })
      Shape("SCATTER-GATHER", s"one account paid ${middles.size} go-betweens, which all paid one account")
    else if (middles.size == 1 && in(middles.head) >= 2 && out(middles.head) >= 2 &&
             onlyBetween { case (a, b) => a == middles.head || b == middles.head })
      Shape("GATHER-SCATTER", s"${in(middles.head)} accounts paid one, which paid ${out(middles.head)} others")
    else if (middles.isEmpty && sources.size >= 2 && sinks.size >= 2)
      Shape("BIPARTITE", s"${sources.size} accounts paid ${sinks.size} others")
    else order match {
      case None => Shape("RANDOM", s"an irregular path through ${nodes.size} accounts, with a loop")
      case Some(sorted) =>
        // Each account's layer: the longest run of payments leading to it
        val layer = mutable.Map(nodes.map(_ -> 0): _*)
        sorted.foreach(a => edges.filter(_._1 == a).foreach { case (_, b) => layer(b) = math.max(layer(b), layer(a) + 1) })
        val layers  = layer.values.max + 1
        val widest  = layer.values.groupBy(identity).values.map(_.size).max
        val stacked = onlyBetween { case (a, b) => layer(b) == layer(a) + 1 }
        if (stacked && layers >= 3 && widest >= 2) Shape("STACK", s"the money passed through $layers layers of up to $widest accounts")
        else if (widest == 1) Shape("RANDOM", s"a chain of payments through ${nodes.size} accounts")
        else Shape("RANDOM", s"an irregular path through ${nodes.size} accounts")
    }
  }

  /** The accounts in an order where every payment goes forwards, or None if the money goes round a loop. */
  private def topologicalOrder(nodes: Seq[String], edges: Seq[(String, String)]): Option[Seq[String]] = {
    val waiting = mutable.Map(nodes.map(n => n -> edges.count(_._2 == n)): _*)
    val ready   = mutable.Queue(nodes.filter(waiting(_) == 0): _*)
    val sorted  = mutable.ArrayBuffer.empty[String]
    while (ready.nonEmpty) {
      val a = ready.dequeue()
      sorted += a
      edges.filter(_._1 == a).foreach { case (_, b) =>
        waiting(b) -= 1
        if (waiting(b) == 0) ready.enqueue(b)
      }
    }
    if (sorted.size == nodes.size) Some(sorted.toSeq) else None
  }
}
