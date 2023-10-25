package com.qwery.runtime.instructions.infrastructure

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, QweryCompiler, Scope}
import org.scalatest.funspec.AnyFunSpec

class DropTest extends AnyFunSpec with VerificationTools {

  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Drop].getSimpleName) {

    it("should compile: drop if exists") {
      val results = compiler.compile(
        """|drop if exists computeDSQ
           |""".stripMargin)
      assert(results == Drop(ref = DatabaseObjectRef("computeDSQ"), ifExists = true))
    }

    it("should compile: drop") {
      val results = compiler.compile(
        """|drop computeDSQ
           |""".stripMargin)
      assert(results == Drop(ref = DatabaseObjectRef("computeDSQ"), ifExists = false))
    }

    it("should decompile: drop if exists") {
      verify(
        """|drop if exists computeDSQ
           |""".stripMargin)
    }

    it("should decompile: drop") {
      verify(
        """|drop computeDSQ
           |""".stripMargin)
    }

    it("should execute: drop if exists") {
      val (_, cost, _) = QweryVM.executeSQL(Scope(),
        """|drop if exists @@stocks
           |""".stripMargin)
      assert(cost.destroyed == 1)
    }

    it("should execute: drop") {
      val (_, cost, _) = QweryVM.executeSQL(Scope(),
        """|create table if not exists dummyTable(id: Int)
           |drop dummyTable
           |""".stripMargin)
      assert(cost.destroyed == 1)
    }

  }

}
