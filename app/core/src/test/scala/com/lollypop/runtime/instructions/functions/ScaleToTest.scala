package com.lollypop.runtime.instructions.functions

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Scale Test Suite
 */
class ScaleToTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val ctx: LollypopUniverse = LollypopUniverse()

  describe(classOf[ScaleTo].getSimpleName) {

    it("should execute the 'scaleTo()' function") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),"select value: scaleTo(1.53451, 4)")
      results.toMapGraph.zipWithIndex foreach { case (row, n) => logger.info(f"[$n%02d] $row") }
      assert(results.toMapGraph.headOption.flatMap(_.get("value")).contains(1.5345))
    }

  }

}
