package com.qwery.runtime.instructions.queryables

import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class ExposeTest extends AnyFunSpec {

  describe(classOf[Expose].getSimpleName) {

    it("should expose the components of a `matches` expression") {
      val (_, _, results) = QweryVM.searchSQL(Scope(),
        """|val isNumber = (x: Any) => x.isNumber()
           |val isUUID = (x: Any) => x.isUUID()
           |val isString = (x: Any) => x.isString()
           |
           |val response = [{ id: '123456789', symbol: 'AAPL', exchange: 'NYSE', lastSale: 87.99 }]
           |expose(response like [{ id: isUUID, symbol: isString, exchange: isString, lastSale: isNumber }])
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("expression" -> "(x: Any) => x.isUUID()", "value" -> "\"123456789\"", "result" -> false),
        Map("expression" -> "(x: Any) => x.isString()", "value" -> "\"AAPL\"", "result" -> true),
        Map("expression" -> "(x: Any) => x.isString()", "value" -> "\"NYSE\"", "result" -> true),
        Map("expression" -> "(x: Any) => x.isNumber()", "value" -> "87.99", "result" -> true)
      ))
    }

  }

}
