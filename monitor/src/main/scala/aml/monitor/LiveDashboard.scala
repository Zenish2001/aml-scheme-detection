package aml.monitor

import aml.common.{RuleParser, ScoringModel}
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}
import akka.util.Timeout
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.io.{Source => FileSource}

/** Part 7c: the live dashboard. The same stream as LiveMonitor, paced so a person can watch it, served to a web
  * page by Akka HTTP. Alerts reach the page as they happen (server-sent events), with running totals four times
  * a second. The page can pause the stream, change its speed and move the alert threshold while it runs, and
  * asks the CaseKeeper actor for cases, and for one case in full to draw it.
  *
  * Run it from the sbt shell, after ReplayExportMain:
  *   monitor/runMain aml.monitor.LiveDashboard                             (HI-Small's test days, 20,000 a second)
  *   monitor/runMain aml.monitor.LiveDashboard data/replay/LI-Small 5000   (another dataset, another pace)
  * then open http://localhost:8080. The replay starts when the page opens; stop it with the page's button.
  */
object LiveDashboard {

  val Port              = 8080
  val DefaultRate       = 20000
  val MinRate           = 100
  val MaxRate           = 100000
  val StayUpAfterFinish = 15.minutes

  def main(args: Array[String]): Unit = {
    val replay = args.headOption.getOrElse(LiveMonitor.DefaultReplay)
    val rate   = args.lift(1).flatMap(_.toIntOption).getOrElse(DefaultRate)

    val loaded = for {
      model    <- ScoringModel.load(LiveMonitor.TreesFile).left.map(e => s"${LiveMonitor.TreesFile}: $e (run ModelExportMain first)")
      settings <- MonitorSettings.load(LiveMonitor.SettingsFile).left.map(e => s"${LiveMonitor.SettingsFile}: $e (run ReplayExportMain first)")
      reasons  <- RuleParser.parseFile(LiveMonitor.ReasonsFile).left.map(e => s"${LiveMonitor.ReasonsFile}: $e")
    } yield (Assessor(model, settings, reasons), settings)

    loaded match {
      case Left(error)                 => println(s"\nThe dashboard can't start: $error")
      case Right((assessor, settings)) => run(replay, math.max(MinRate, math.min(MaxRate, rate)), assessor, settings)
    }
  }

  private def run(replay: String, rate: Int, assessor: Assessor, settings: MonitorSettings): Unit = {
    implicit val system: ActorSystem[CaseKeeper.Command] = ActorSystem(CaseKeeper(), "aml-dashboard")
    implicit val timeout: Timeout = 10.seconds
    import system.executionContext

    val controls = new Controls(rate, settings.threshold)

    // Running totals, written by the stream and read by the page; "since" counts restart when the threshold moves
    val payments, alerts, caught, laundering, nanos, minute, sinceAlerts, sinceCaught, sinceMinute, speed, lastCount =
      new AtomicLong(0)
    val time     = new AtomicReference("")
    val finished = new AtomicBoolean(false)
    val opened   = Promise[Unit]()

    system.scheduler.scheduleAtFixedRate(1.second, 1.second)(() => {
      val n = payments.get
      speed.set(n - lastCount.getAndSet(n))
    })

    def snapshot(): Snapshot = {
      val n = payments.get
      Snapshot(n, alerts.get, caught.get, laundering.get, minute.get, time.get,
        if (n == 0) 0.0 else nanos.get / 1000.0 / n, speed.get, finished.get, replay, controls.rate, controls.paused,
        controls.threshold, controls.defaultThreshold, sinceAlerts.get, sinceCaught.get,
        if (sinceMinute.get == 0) 0 else minute.get - sinceMinute.get)
    }

    def summary(): Future[CaseKeeper.Summary] = system.ask[CaseKeeper.Summary](ref => CaseKeeper.Report(ref))
    def json(body: String) = HttpEntity(ContentTypes.`application/json`, body)

    // Alerts go into a queue; the hub hands each one to every page that's open
    val (alertQueue, alertHub) = Source.queue[String](4096).toMat(BroadcastHub.sink[String](1024))(Keep.both).run()

    val route: Route = concat(
      pathSingleSlash(get(respondWithHeader(RawHeader("Cache-Control", "no-store"))(getFromResource("dashboard/index.html")))),
      path("events")(get {
        opened.trySuccess(())
        val totals = Source.tick(0.millis, 250.millis, ())
          .map(_ => ServerSentEvent(Dashboard.statsJson(snapshot()), "stats"))
          .mapMaterializedValue(_ => NotUsed)
        complete(alertHub.map(a => ServerSentEvent(a, "alert")).merge(totals)
          .keepAlive(10.seconds, () => ServerSentEvent.heartbeat))
      }),
      path("cases")(get(onSuccess(summary())(s => complete(json(Dashboard.casesJson(s)))))),
      path("case")(get(parameters("key".as[Long]) { key =>
        onSuccess(summary()) { s =>
          s.cases.find(_.alerts.exists(_.payment.txnId == key)) match {
            case Some(c) => complete(json(Dashboard.caseJson(c)))
            case None    => complete(StatusCodes.NotFound, "That case isn't in the monitor any more")
          }
        }
      })),
      path("control")(post(parameters("pause".as[Boolean].optional, "rate".as[Int].optional, "threshold".as[Double].optional) {
        (pause, newRate, newThreshold) =>
          pause.foreach(p => if (p) controls.pause() else controls.resume())
          newRate.foreach(r => controls.rate = math.max(MinRate, math.min(MaxRate, r)))
          newThreshold.foreach { t =>
            controls.threshold = math.max(0.0, math.min(1.0, t))
            sinceAlerts.set(0); sinceCaught.set(0); sinceMinute.set(minute.get)
          }
          complete(json(Dashboard.statsJson(snapshot())))
      })),
      path("stop")(post {
        system.scheduler.scheduleOnce(500.millis, () => system.terminate())
        complete("Stopping the monitor")
      }))

    Await.result(Http().newServerAt("localhost", Port).bind(route), 10.seconds)
    println(s"\nThe dashboard is ready: open http://localhost:$Port in your browser, and the replay starts.")
    println("Stop it with the page's button; it also stops on its own 15 minutes after the replay ends.")

    // The replay starts when the page first opens, so no alert is missed
    opened.future.foreach { _ =>
      val input = FileSource.fromFile(Replay.file(replay), "UTF-8")
      val lines = input.getLines()
      val parse = Replay.parser(lines.next())
      Source.fromIterator(() => lines).map(parse)
        // The cost of each payment depends on the chosen speed, so the speed can change while it runs
        .throttle(MaxRate, 1.second, MaxRate, (_: LivePayment) => math.max(1, MaxRate / math.max(1, controls.rate)), ThrottleMode.Shaping)
        .mapAsync(1)(p => if (!controls.paused) Future.successful(p) else controls.whenRunning.map(_ => p))
        .map(p => assessor.assessAt(p, controls.threshold))
        .runWith(Sink.foreach { d =>
          payments.incrementAndGet()
          nanos.addAndGet(d.nanos)
          minute.set(d.payment.minute)
          time.set(d.payment.time)
          if (sinceMinute.get == 0) sinceMinute.set(d.payment.minute)
          if (d.payment.isLaundering) laundering.incrementAndGet()
          if (d.isAlert) {
            alerts.incrementAndGet(); sinceAlerts.incrementAndGet()
            if (d.payment.isLaundering) { caught.incrementAndGet(); sinceCaught.incrementAndGet() }
            system ! CaseKeeper.Alerted(d)
            alertQueue.offer(Dashboard.alertJson(d))
          }
        })
        .onComplete { _ =>
          finished.set(true)
          input.close()
          println(f"\nThe replay has ended: ${payments.get}%,d payments, ${alerts.get}%,d alerts. The page stays up for 15 minutes.")
          system.scheduler.scheduleOnce(StayUpAfterFinish, () => system.terminate())
        }
    }

    Await.result(system.whenTerminated, Duration.Inf)
    println("The dashboard has stopped.")
  }
}
