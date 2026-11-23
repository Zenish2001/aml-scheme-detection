package aml.monitor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DashboardSpec extends AnyFlatSpec with Matchers {

  private def alert(id: Long, from: String, to: String, real: Boolean, features: Map[String, Any] = Map("amountPaidUsd" -> 100.0)) =
    Decision(LivePayment(id, id, s"2022-09-07 00:0$id", from, to, "ACH", isLaundering = real, scheme = None, features = features),
      score = 0.99, isAlert = true, reasons = Nil, nanos = 0)

  "Dashboard.statsJson" should "write the running totals and the controls as JSON" in {
    val s = Snapshot(1000, 5, 4, 9, 27703291, "2022-09-07 05:51", 2.8, 19876, finished = false, "data/replay/HI-Small", 20000,
      paused = true, threshold = 0.95, defaultThreshold = 0.943, sinceAlerts = 2, sinceCaught = 2, sinceMinutes = 90)
    Dashboard.statsJson(s) shouldBe
      """{"payments":1000,"alerts":5,"caught":4,"laundering":9,"minute":27703291,"time":"2022-09-07 05:51",""" +
        """"averageMicros":2.80,"speed":19876,"finished":false,"source":"data/replay/HI-Small","rate":20000,"paused":true,""" +
        """"threshold":0.95,"defaultThreshold":0.943,"sinceAlerts":2,"sinceCaught":2,"sinceMinutes":90}"""
  }

  "Dashboard.alertJson" should "include the facts behind the reasons, when the payment has them" in {
    val a = alert(1, "A", "B", real = true, Map("amountPaidUsd" -> 7500.0, "pairPayments7d" -> 0.0, "format" -> "ACH"))
    Dashboard.alertJson(a.copy(reasons = Seq("New payee" -> 1.7))) shouldBe
      """{"txnId":1,"time":"2022-09-07 00:01","sender":"A","receiver":"B","amountUsd":7500.0,"format":"ACH","score":0.99,""" +
        """"reasons":[{"text":"New payee","lift":1.7}],"facts":{"pairPayments7d":0.0},"isLaundering":true,"scheme":null}"""
  }

  "Dashboard.casesJson and caseJson" should "list cases with a key that stays the same, and give one case in full" in {
    val summary = CaseKeeper.summarize(Seq(alert(1, "A", "B", real = true), alert(2, "A", "C", real = false), alert(3, "X", "Y", real = true)))

    Dashboard.casesJson(summary) shouldBe
      """{"total":2,"cases":[""" +
        """{"key":1,"id":1,"scheme":"FAN-OUT","why":"one account paid 2 others","alerts":2,"real":1,"first":"2022-09-07 00:01"},""" +
        """{"key":3,"id":2,"scheme":"RANDOM","why":"a chain of payments through 2 accounts","alerts":1,"real":1,"first":"2022-09-07 00:03"}]}"""
    val one = Dashboard.caseJson(summary.cases(1))
    one should startWith(
      """{"key":3,"scheme":"RANDOM","why":"a chain of payments through 2 accounts","payments":[""" +
        """{"txnId":3,"time":"2022-09-07 00:03","sender":"X","receiver":"Y","amountUsd":100.0,"format":"ACH","score":0.99,"real":true}],""")
    one should include(""""sar":{"accounts":2,"banks":2,"payments":1,"totalUsd":100.0,"first":"2022-09-07 00:03",""")
    one should include(""""formats":[{"format":"ACH","count":1}],"reasons":[],"narrative":"On 2022-09-07 00:03, 2 accounts""")
  }

  "Dashboard.rulesJson" should "write the rule-engine actor's counts, the model first" in {
    val r = RuleEngine.Stats(10, 3, 2, 2, Seq(RuleEngine.RuleStat("New payee", 4, 1, 1)))
    Dashboard.rulesJson(r) shouldBe
      """{"payments":10,"laundering":3,"modelAlerts":2,"modelReal":2,"rules":[{"name":"New payee","alerts":4,"real":1,"alsoModel":1}]}"""
  }

  "Controls" should "pause and resume the stream, and keep the speed and threshold" in {
    val c = new Controls(20000, 0.943)
    c.paused shouldBe false
    c.whenRunning.isCompleted shouldBe true
    c.pause()
    c.paused shouldBe true
    val waiting = c.whenRunning
    waiting.isCompleted shouldBe false
    c.resume()
    waiting.isCompleted shouldBe true
    c.paused shouldBe false
    c.threshold shouldBe 0.943
  }

  "Dashboard.quote" should "escape quotes, backslashes and control characters" in {
    Dashboard.quote("a\"b\\c\n") shouldBe "\"a\\\"b\\\\c\\u000a\""
  }
}
