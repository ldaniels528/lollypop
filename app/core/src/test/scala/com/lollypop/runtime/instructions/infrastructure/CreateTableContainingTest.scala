package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.language.models.TableModel
import com.lollypop.runtime.implicits.risky._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.{RowsOfValues, Select}
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class CreateTableContainingTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()
  private val securitiesRef = DatabaseObjectRef(getTestTableName)
  private val specialSecuritiesRef = DatabaseObjectRef(getTestTableName + "_SS")

  describe(classOf[CreateTableContaining].getSimpleName) {

    it("should compile create table containing inline values") {
      val results = compiler.compile(
       s"""|create table if not exists $specialSecuritiesRef (symbol: String, lastSale: Double)
           |containing values ('AAPL', 202.11), ('AMD', 23.50), ('GOOG', 765.33), ('AMZN', 699.01)
           |""".stripMargin)
      assert(results == CreateTableContaining(specialSecuritiesRef,
        tableModel = TableModel(columns = List("symbol: String".c, "lastSale: Double".c)),
        from = RowsOfValues(List(List("AAPL", 202.11), List("AMD", 23.50), List("GOOG", 765.33), List("AMZN", 699.01))),
        ifNotExists = true)
      )
    }

    it("should compile create table containing sub-query") {

      val results = compiler.compile(
        s"""|create table $specialSecuritiesRef (symbol: String, lastSale: Double)
            |containing (
            |    select symbol, lastSale
            |    from $securitiesRef
            |    where useCode == 'SPECIAL'
            |)
            |""".stripMargin)
      assert(results == CreateTableContaining(specialSecuritiesRef,
        tableModel = TableModel(columns = List("symbol: String".c, "lastSale: Double".c)),
        from = Select(fields = Seq("symbol".f, "lastSale".f), from = Some(securitiesRef), where = "useCode".f === "SPECIAL"),
        ifNotExists = false)
      )
    }

    it("should decompile create table containing inline values") {
      verify(
       s"""|create table $specialSecuritiesRef (symbol string, lastSale double)
           |containing values ('AAPL', 202.11), ('AMD', 23.50), ('GOOG', 765.33), ('AMZN', 699.01)
           |""".stripMargin)
    }

    it("should decompile create table containing sub-query") {
      verify(
       s"""|create table $specialSecuritiesRef (symbol: String(10), lastSale double)
           |containing (
           |    select symbol, lastSale
           |    from Securities
           |    where useCode == 'SPECIAL'
           |)
           |""".stripMargin)
    }

    it("should execute create table using inline values") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|drop if exists $specialSecuritiesRef
            |create table $specialSecuritiesRef (symbol: String(10), lastSale: Double)
            |containing values ('AAPL', 202.11), ('AMD', 23.50), ('GOOG', 765.33), ('AMZN', 699.01)
            |select * from $specialSecuritiesRef
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAPL", "lastSale" -> 202.11),
        Map("symbol" -> "AMD", "lastSale" -> 23.5),
        Map("symbol" -> "GOOG", "lastSale" -> 765.33),
        Map("symbol" -> "AMZN", "lastSale" -> 699.01)
      ))
    }

  }

}
