package com.qwery.runtime.instructions.functions

import com.qwery.language.QweryUniverse
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Scale Test Suite
 */
class ScaleToTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val ctx: QweryUniverse = QweryUniverse()

  describe(classOf[ScaleTo].getSimpleName) {

    it("should execute the 'scaleTo()' function") {
      val (_, _, results) = QweryVM.searchSQL(Scope(),"select value: scaleTo(1.53451, 4)")
      results.toMapGraph.zipWithIndex foreach { case (row, n) => logger.info(f"[$n%02d] $row") }
      assert(results.toMapGraph.headOption.flatMap(_.get("value")).contains(1.5345))
    }

  }

}
