package com.qwery.language

import org.scalatest.funspec.AnyFunSpec

class QweryUniverseTest extends AnyFunSpec {

  describe(classOf[QweryUniverse].getSimpleName) {

    it("should provide a root scope with pre-populated values") {
      val qu = QweryUniverse()
      val scope = qu.createRootScope()
      assert(scope.resolveAs[Double]("π") contains Math.PI)
    }

  }

}
