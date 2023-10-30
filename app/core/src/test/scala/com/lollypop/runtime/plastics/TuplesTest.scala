package com.lollypop.runtime.plastics

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.plastics.Tuples.seqToTuple
import com.lollypop.runtime.{DynamicClassLoader, Scope}
import org.scalatest.funspec.AnyFunSpec

class TuplesTest extends AnyFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = ctx.createRootScope()
  implicit val cl: DynamicClassLoader = ctx.classLoader

  describe(classOf[Tuples.type].getSimpleName) {

    it("should convert a sequence into a tuple") {
      assert(seqToTuple((1 to 22).toList) contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22))
    }

  }

}
