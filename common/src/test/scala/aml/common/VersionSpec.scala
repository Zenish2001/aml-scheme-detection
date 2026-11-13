package aml.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VersionSpec extends AnyFlatSpec with Matchers {
  "Version" should "name the project" in {
    Version.name shouldBe "AML Scheme Detection"
  }
}
