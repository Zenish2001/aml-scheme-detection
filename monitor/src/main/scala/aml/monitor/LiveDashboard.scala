package aml.monitor

import aml.common.{FeatureEngine, IbmCsv, RawPayment, Rule, RuleParser, ScoringModel}
import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.{ActorAttributes, KillSwitches, ThrottleMode, UniqueKillSwitch}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}
import akka.util.Timeout
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.io.{Source => FileSource}
import scala.util.{Failure, Success, Using}

/** Parts 7c and 8d: the live dashboard. It streams payments as LiveMonitor does, paced so a person can watch, and
  * serves a web page with Akka HTTP: alerts as they happen (server-sent events), running totals four times a
  * second, cases from the CaseKeeper actor, and the rules from the rule-engine actor. The page can pause, change
  * the speed and move the alert threshold while it runs.
  *
  * It starts by replaying the test days; uploading a file of payments in IBM's format (Part 8d) stops that and
  * checks the file instead, with its features computed live by the FeatureEngine, so no Spark is needed.
  *
  * Run it from the sbt shell, after ReplayExportMain:
  *   monitor/runMain aml.monitor.LiveDashboard                             (HI-Small's test days, 20,000 a second)
  *   monitor/runMain aml.monitor.LiveDashboard data/replay/LI-Small 5000   (another replay, another pace)
  * then open http://127.0.0.1:8080. The replay starts when the page opens; stop it with the page's button.
  */
object LiveDashboard {

  val Port              = 8080
  val DefaultRate       = 20000
  val MinRate           = 100
  val MaxRate           = 100000
  val StayUpAfterFinish = 15.minutes
  val MaxUploadBytes    = 250L * 1024 * 1024
  val MaxUploadPayments = 1500000
  val MaxHourPayments   = 150000   // ordinary hours hold far fewer; IBM's start-up hour holds hundreds of thousands
  private val ResetMark = "\u0000reset"
  /** Reading files from disk, and computing features, run here, away from the web server's own threads. */
  private val Blocking  = ActorAttributes.dispatcher("akka.actor.default-blocking-io-dispatcher")

  def main(args: Array[String]): Unit = {
    val replay = args.headOption.getOrElse(LiveMonitor.DefaultReplay)
    val rate   = args.lift(1).flatMap(_.toIntOption).getOrElse(DefaultRate)

    val loaded = for {
      model    <- ScoringModel.load(LiveMonitor.TreesFile).left.map(e => s"${LiveMonitor.TreesFile}: $e (run ModelExportMain first)")
      settings <- MonitorSettings.load(LiveMonitor.SettingsFile).left.map(e => s"${LiveMonitor.SettingsFile}: $e (run ReplayExportMain first)")
      reasons  <- RuleParser.parseFile(LiveMonitor.ReasonsFile).left.map(e => s"${LiveMonitor.ReasonsFile}: $e")
      rules    <- RuleParser.parseFile(LiveMonitor.RulesFile).left.map(e => s"${LiveMonitor.RulesFile}: $e")
      usdPer   <- UsdRates.load()
    } yield (Assessor(model, settings, reasons), settings, rules, usdPer)

    loaded match {
      case Left(error) => println(s"\nThe dashboard can't start: $error")
      case Right((assessor, settings, rules, usdPer)) =>
        run(replay, math.max(MinRate, math.min(MaxRate, rate)), assessor, settings, rules, usdPer)
    }
  }

  private def run(replay: String, rate: Int, assessor: Assessor, settings: MonitorSettings, rules: Seq[Rule],
                  usdPer: Map[String, Double]): Unit = {
    implicit val system: ActorSystem[CaseKeeper.Command] = ActorSystem(CaseKeeper(), "aml-dashboard")
    implicit val timeout: Timeout = 10.seconds
    import system.executionContext
    val ruleEngine = system.systemActorOf(RuleEngine(rules), "rule-engine")
    val controls   = new Controls(rate, settings.threshold)

    // Running totals, written by the stream and read by the page; "since" counts restart when the threshold moves
    val payments, alerts, caught, laundering, nanos, minute, sinceAlerts, sinceCaught, sinceMinute, speed, lastCount =
      new AtomicLong(0)
    val counters = Seq(payments, alerts, caught, laundering, nanos, minute, sinceAlerts, sinceCaught, sinceMinute, speed, lastCount)
    val time     = new AtomicReference("")
    val finished = new AtomicBoolean(false)
    val source   = new AtomicReference(replay)
    val labelled = new AtomicBoolean(true)
    val runId    = new AtomicLong(0)                                    // which run (replay or file) is current
    val current  = new AtomicReference[Option[UniqueKillSwitch]](None)
    val opened   = Promise[Unit]()
    val reading  = new AtomicBoolean(false)                             // an uploaded file is being read
    val latest   = new AtomicLong(0)                                    // the most recent request, upload or replay: it wins
    val uploads  = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor(r => { val t = new Thread(r, "upload-reader"); t.setDaemon(true); t }))

    system.scheduler.scheduleAtFixedRate(1.second, 1.second)(() => {
      val n = payments.get
      speed.set(n - lastCount.getAndSet(n))
    })

    def snapshot(): Snapshot = {
      val n = payments.get
      Snapshot(n, alerts.get, caught.get, laundering.get, minute.get, time.get,
        if (n == 0) 0.0 else nanos.get / 1000.0 / n, speed.get, finished.get, source.get, controls.rate, controls.paused,
        controls.threshold, controls.defaultThreshold, sinceAlerts.get, sinceCaught.get,
        if (sinceMinute.get == 0) 0 else minute.get - sinceMinute.get, labelled.get)
    }

    def summary(): Future[CaseKeeper.Summary] = system.ask[CaseKeeper.Summary](ref => CaseKeeper.Report(ref))
    def json(body: String) = HttpEntity(ContentTypes.`application/json`, body)

    // Alerts go into a queue; the hub hands each one to every page that's open
    val (alertQueue, alertHub) = Source.queue[String](4096).toMat(BroadcastHub.sink[String](1024))(Keep.both).run()

    /** Stops whatever is running, clears every count, case and rule tally, and checks these payments instead. */
    def start(name: String, hasLabels: Boolean, input: Source[LivePayment, NotUsed], onEnd: () => Unit): Unit = synchronized {
      current.getAndSet(None).foreach(_.shutdown())
      val id = runId.incrementAndGet()
      counters.foreach(_.set(0))
      time.set(""); finished.set(false); source.set(name); labelled.set(hasLabels)
      system ! CaseKeeper.Reset
      ruleEngine ! RuleEngine.Reset(id)
      controls.resume()
      alertQueue.offer(ResetMark + s"""{"source":${Dashboard.quote(name)},"labelled":$hasLabels}""")

      val (switch, done) = input
        // The cost of each payment depends on the chosen speed, so the speed can change while it runs
        .throttle(MaxRate, 1.second, MaxRate, (_: LivePayment) => math.max(1, MaxRate / math.max(1, controls.rate)), ThrottleMode.Shaping)
        .mapAsync(1)(p => if (!controls.paused) Future.successful(p) else controls.whenRunning.map(_ => p))
        .map(p => assessor.assessAt(p, controls.threshold))
        // The rule-engine actor checks each batch against the rules, beside the model
        .groupedWithin(200, 100.millis)
        .mapAsync(1)(batch => ruleEngine.ask[Done](ref => RuleEngine.CheckBatch(batch, ref, id)).map(_ => batch))
        .mapConcat(identity)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.foreach { d =>
          if (runId.get == id) {
            val n = payments.incrementAndGet()
            if (n % 100000 == 0) println(f"  $name: $n%,d payments checked, now at ${d.payment.time}")
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
          }
        })(Keep.both)
        .run()
      current.set(Some(switch))

      println(s"Started checking $name")
      done.onComplete { result =>
        onEnd()
        result match {
          case Failure(e) =>
            println(s"\n$name STOPPED WITH AN ERROR after ${payments.get} payments: $e")
            e.printStackTrace()
          case Success(_) => ()
        }
        if (runId.get == id) {
          finished.set(true)
          println(f"\n$name: all ${payments.get}%,d payments checked, ${alerts.get}%,d alerts. The page stays up for 15 minutes.")
          system.scheduler.scheduleOnce(StayUpAfterFinish, () => if (runId.get == id) system.terminate())
        }
      }
    }

    /** The test days' replay file, as at the start. */
    def replayFile(): Unit = {
      val input = FileSource.fromFile(Replay.file(replay), "UTF-8")
      val lines = input.getLines()
      val parse = Replay.parser(lines.next())
      start(replay.split('/').filter(_.nonEmpty).last, hasLabels = true,
        Source.fromIterator(() => lines).map(parse).withAttributes(Blocking).async, () => input.close())
    }

    /** An uploaded file's payments, with their features computed live, as the monitor would see them arrive. */
    def checkFile(name: String, parsed: IbmCsv.Parsed, hubs: Set[String]): Unit = {
      def live(p: RawPayment, f: Map[String, Double], label: Boolean) = LivePayment(p.id, p.minute, IbmCsv.timeText(p.minute),
        p.sender, p.receiver, p.format, isLaundering = label, scheme = None, features = (f: Map[String, Any]) + ("format" -> p.format))
      start(name, parsed.labelled, Source.fromIterator { () =>
        // The engine gives payments back in the order it's given them, so each label can go along with its payment
        val labels = parsed.rows.iterator
        FeatureEngine.run(parsed.rows.iterator.map(_.payment), usdPer, hubs).map { case (p, f) =>
          val row = labels.next()
          require(row.payment.id == p.id, "the engine changed the payments' order")
          live(p, f, row.isLaundering.getOrElse(false))
        }
      }.withAttributes(Blocking).async, () => ())
    }

    val route: Route = concat(
      pathSingleSlash(get(respondWithHeader(RawHeader("Cache-Control", "no-store"))(getFromResource("dashboard/index.html")))),
      path("events")(get {
        opened.trySuccess(())
        val totals = Source.tick(0.millis, 250.millis, ())
          .map(_ => ServerSentEvent(Dashboard.statsJson(snapshot()), "stats"))
          .mapMaterializedValue(_ => NotUsed)
        val events = alertHub.map { a =>
          if (a.startsWith(ResetMark)) ServerSentEvent(a.drop(ResetMark.length), "reset") else ServerSentEvent(a, "alert")
        }
        complete(events.merge(totals).keepAlive(10.seconds, () => ServerSentEvent.heartbeat))
      }),
      path("cases")(get(onSuccess(summary())(s => complete(json(Dashboard.casesJson(s)))))),
      path("rules")(get(onSuccess(ruleEngine.ask[RuleEngine.Stats](ref => RuleEngine.Report(ref)))(r => complete(json(Dashboard.rulesJson(r)))))),
      path("case")(get(parameters("key".as[Long]) { key =>
        onSuccess(summary()) { s =>
          s.cases.find(_.alerts.exists(_.payment.txnId == key)) match {
            case Some(c) => complete(json(Dashboard.caseJson(c)))
            case None    => complete(StatusCodes.NotFound, "That case isn't in the monitor any more")
          }
        }
      })),
      path("upload")(post(withSizeLimit(MaxUploadBytes)(storeUploadedFile("file", _ => File.createTempFile("aml-upload-", ".csv")) { case (info, file) =>
        if (!reading.compareAndSet(false, true)) {
          file.delete()
          complete(StatusCodes.Conflict, json("""{"error":"Still reading the last file: try again in a moment."}"""))
        } else {
          val ticket = latest.incrementAndGet()
          println(f"\nReceived ${info.fileName} (${file.length / 1e6}%.1f MB); reading it...")
          val checked = Future {
            val began = System.nanoTime()
            try Using.resource(FileSource.fromFile(file, "UTF-8"))(in => IbmCsv.parse(in.getLines(), usdPer.keySet, MaxUploadPayments, maxPerHour = MaxHourPayments))
              .map { parsed =>
                println(f"  read ${parsed.rows.size}%,d payments in ${(System.nanoTime() - began) / 1e9}%.1f s; finding hub accounts...")
                val hubs = FeatureEngine.hubs(parsed.rows.iterator.map(_.payment))
                println(f"  ${hubs.size}%,d hubs found")
                parsed -> hubs
              }
            finally file.delete()
          }(uploads)
          checked.onComplete {
            case Failure(e) => reading.set(false); println(s"  READING ${info.fileName} FAILED: $e"); e.printStackTrace()
            case Success(Left(problem)) => reading.set(false); println(s"  ${info.fileName} refused: $problem")
            case Success(Right(_)) => reading.set(false)
          }
          onSuccess(checked) {
          case Left(problem) => complete(StatusCodes.BadRequest, json(s"""{"error":${Dashboard.quote(problem)}}"""))
          case Right(_) if latest.get != ticket =>
            println(s"  ${info.fileName} not started: the test days' replay was asked for while it was being read")
            complete(StatusCodes.Conflict, json("""{"error":"You went back to the test days while this file was being read, so it wasn't started."}"""))
          case Right((parsed, hubs)) =>
            checkFile(info.fileName, parsed, hubs)
            val skippedNote = if (parsed.skipped > 0) f" (${parsed.skipped}%,d lines skipped)" else ""
            println(f"\nChecking ${info.fileName}: ${parsed.rows.size}%,d payments, from ${parsed.first} to ${parsed.last}$skippedNote")
            complete(json(Dashboard.uploadJson(info.fileName, parsed, hubs.size)))
          }
        }
      }))),
      path("replay")(post {
        latest.incrementAndGet()
        replayFile()
        complete(json(Dashboard.statsJson(snapshot())))
      }),
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
    println(s"\nThe dashboard is ready: open http://127.0.0.1:$Port in your browser, and the replay starts.")
    println("Stop it with the page's button; it also stops on its own 15 minutes after a replay or file ends.")

    // The replay starts when the page first opens, so no alert is missed
    opened.future.foreach(_ => replayFile())

    Await.result(system.whenTerminated, Duration.Inf)
    println("The dashboard has stopped.")
  }
}
