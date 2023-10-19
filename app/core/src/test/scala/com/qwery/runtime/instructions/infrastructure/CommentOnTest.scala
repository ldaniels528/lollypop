package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import qwery.io.IOCost

class CommentOnTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[CommentOn].getSimpleName) {

    it("should compile: comment on if exists stocks as 'just a staging table'") {
      val expected = CommentOn(DatabaseObjectRef("stocks"), "just a staging table".v, ifExists = true)
      val actual = compiler.compile("comment on if exists stocks := 'just a staging table'")
      assert(actual == expected)
    }

    it("should compile: comment on stocks as 'just a staging table'") {
      val expected = CommentOn(DatabaseObjectRef("stocks"), "just a staging table".v, ifExists = false)
      val actual = compiler.compile("comment on stocks := 'just a staging table'")
      assert(actual == expected)
    }

    it("should decompile: comment on if exists stocks as 'just a staging table'") {
      val model = CommentOn(DatabaseObjectRef("stocks"), "just a staging table".v, ifExists = true)
      assert(model.toSQL == """comment on if exists stocks := "just a staging table"""")
    }

    it("should decompile: comment on stocks as 'just a staging table'") {
      val model = CommentOn(DatabaseObjectRef("stocks"), "just a staging table".v, ifExists = false)
      assert(model.toSQL == """comment on stocks := "just a staging table"""")
    }

    it("should execute: comment on if exists stocks as 'just a staging table'") {
      val tableRef = DatabaseObjectRef(getTestTableName)
      val (scope0, cost0) = QweryVM.infrastructureSQL(Scope(),
        s"""|drop if exists $tableRef &&
            |create table $tableRef (
            |   symbol: String(8),
            |   exchange: String(8),
            |   lastSale: Double
            |) &&
            |comment on if exists $tableRef := "just a staging table"
            |""".stripMargin)
      assert(cost0 == IOCost(created = 1, destroyed = 1, updated = 1))

      val (_, _, results1) = QweryVM.searchSQL(scope0,
        s"""|select comment from (OS.getDatabaseObjects()) where qname is "${tableRef.toNS(scope0)}"
            |""".stripMargin)
      assert(results1.toMapGraph == List(Map("comment" -> "just a staging table")))
    }

    it("should execute: comment on stocks as 'just a staging table'") {
      val tableRef = DatabaseObjectRef(getTestTableName)
      val (scope0, cost0) = QweryVM.infrastructureSQL(Scope(),
        s"""|drop if exists $tableRef &&
            |create table $tableRef (
            |   symbol: String(8),
            |   exchange: String(8),
            |   lastSale: Double
            |) &&
            |comment on $tableRef := "just a staging table"
            |""".stripMargin)
      assert(cost0 == IOCost(created = 1, destroyed = 1, updated = 1))

      val (_, _, results1) = QweryVM.searchSQL(scope0,
        s"""|select comment from (OS.getDatabaseObjects()) where qname is "${tableRef.toNS(scope0)}"
            |""".stripMargin)
      assert(results1.toMapGraph == List(Map("comment" -> "just a staging table")))
    }

  }

}
