package aml.monitor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MonitorMainSpec extends AnyFlatSpec with Matchers {
  "The monitor module" should "see the common module" in {
    aml.common.Version.name should not be empty
  }
}
