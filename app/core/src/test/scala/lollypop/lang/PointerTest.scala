package lollypop.lang

import org.scalatest.funspec.AnyFunSpec

import scala.util.Try

/**
 * Pointer Tests
 */
class PointerTest extends AnyFunSpec {

  describe(classOf[Pointer].getSimpleName) {

    it("it should parse text references (e.g. 'pointer(123, 56, 12)')") {
      val ptr = Pointer.parse("pointer(123, 56, 12)")
      assert(ptr.offset == 123 && ptr.allocated == 56 && ptr.length == 12)
    }

    it("it should return the appropriate error (e.g. 'pointer(123, ABC, 12)')") {
      val outcome = Try(Pointer.parse("pointer(123, ABC, 12)"))
      assert(outcome.isFailure && (outcome.failed.toOption.map(_.getMessage) contains "Illegal object reference 'pointer(123, ABC, 12)'"))
    }

  }

}
