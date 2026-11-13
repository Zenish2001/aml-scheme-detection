package aml.batch

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BatchMainSpec extends AnyFlatSpec with Matchers {
  "The batch module" should "see the common module" in {
    aml.common.Version.name should not be empty
  }
}
