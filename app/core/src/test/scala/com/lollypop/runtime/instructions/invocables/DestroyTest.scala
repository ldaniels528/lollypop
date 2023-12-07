package com.lollypop.runtime.instructions.invocables

import com.lollypop.language._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class DestroyTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Destroy].getSimpleName) {

    it("should support compile") {
      val model = LollypopCompiler().compile("destroy x")
      assert(model == Destroy("x".f))
    }

    it("should support decompile") {
      val model = Destroy("x".f)
      assert(model.toSQL == "destroy x")
    }

    it("should remove (and free) an object from the scope") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        """|val c = CLOB("The little brown fox")
           |destroy c
           |""".stripMargin)
      assert(scope.resolve("c").isEmpty)
    }

    it("should remove (and close) a resource from the scope") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        """|val stocks: Table(symbol: String(5), exchange: String(6), lastSale: Double, lastSaleTime: DateTime) =
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    |------------------------------|
           |destroy stocks
           |""".stripMargin)
      assert(scope.resolve("stocks").isEmpty)
    }

  }

}
