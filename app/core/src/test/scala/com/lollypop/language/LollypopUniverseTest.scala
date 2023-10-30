package com.lollypop.language

import org.scalatest.funspec.AnyFunSpec

class LollypopUniverseTest extends AnyFunSpec {

  describe(classOf[LollypopUniverse].getSimpleName) {

    it("should provide a root scope with pre-populated values") {
      val qu = LollypopUniverse()
      val scope = qu.createRootScope()
      assert(scope.resolveAs[Double]("π") contains Math.PI)
    }

  }

}
