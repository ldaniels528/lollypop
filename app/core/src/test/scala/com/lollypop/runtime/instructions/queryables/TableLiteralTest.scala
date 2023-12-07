package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.{TokenIterator, _}
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.TableColumn
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import lollypop.lang.Null
import org.scalatest.funspec.AnyFunSpec

class TableLiteralTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[TableLiteral].getSimpleName) {

    it("supports parsing Table literals (from SQL to tokens)") {
      val query =
        """||------------------------------|
           || symbol | exchange | lastSale |
           ||------------------------------|
           || XYZ    | AMEX     |    31.95 |
           || AAXX   | NYSE     |    56.12 |
           || QED    | NASDAQ   |          |
           || JUNK   | AMEX     |    97.61 |
           ||------------------------------|
           |""".stripMargin
      val tok = TokenIterator(query)
      val results = tok.map(_.value).toList
      assert(results == List(
        Nil,
        List("symbol", "exchange", "lastSale"),
        Nil,
        List("XYZ", "AMEX", "31.95"),
        List("AAXX", "NYSE", "56.12"),
        List("QED", "NASDAQ", ""),
        List("JUNK", "AMEX", "97.61"),
        Nil
      ))
    }

    it("supports compiling Table literals (from SQL to model)") {
      val query =
        """||------------------------------|
           || symbol | exchange | lastSale |
           ||------------------------------|
           || XYZ    | AMEX     |    31.95 |
           || AAXX   | NYSE     |    56.12 |
           || QED    | NASDAQ   |          |
           || JUNK   | AMEX     |    97.61 |
           ||------------------------------|
           |""".stripMargin
      val results = compiler.compile(query)
      assert(results == TableLiteral(
        columns = List(
          TableColumn(name = "symbol", `type` = StringType(4)),
          TableColumn(name = "exchange", `type` = StringType(6)),
          TableColumn(name = "lastSale", `type` = Float64Type)),
        value = List(
          List("XYZ".v, "AMEX".v, 31.95.v),
          List("AAXX".v, "NYSE".v, 56.12.v),
          List("QED".v, "NASDAQ".v, Null()),
          List("JUNK".v, "AMEX".v, 97.61.v)
        )))
    }

    it("supports de-compiling Table literals (from model to SQL)") {
      val model = TableLiteral(
        columns = List(
          TableColumn(name = "symbol", `type` = StringType(4)),
          TableColumn(name = "exchange", `type` = StringType(6)),
          TableColumn(name = "lastSale", `type` = Float64Type)),
        value = List(
          List("XYZ".v, "AMEX".v, 31.95.v),
          List("AAXX".v, "NYSE".v, 56.12.v),
          List("QED".v, "NASDAQ".v, Null()),
          List("JUNK".v, "AMEX".v, 97.61.v)
        ))
      assert(model.toSQL.trim ==
        """||------------------------------|
           || symbol | exchange | lastSale |
           ||------------------------------|
           || "XYZ"  | "AMEX"   | 31.95    |
           || "AAXX" | "NYSE"   | 56.12    |
           || "QED"  | "NASDAQ" | null     |
           || "JUNK" | "AMEX"   | 97.61    |
           ||------------------------------|
           |""".stripMargin.trim)
    }

    it("should support Table literals as expressions") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|val stocks =
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | QED    | NASDAQ   |          |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    | ABC    | OTCBB    |    5.887 |
           |    |------------------------------|
           |stocks
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "QED", "exchange" -> "NASDAQ"),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887)
      ))
    }

    it("should support Table literals with arrays") {
      val model = compiler.compile(
        """||--------------------------------------------------------------------------|
           || exchange | symbol | codes          | lastSale | lastSaleTime             |
           ||--------------------------------------------------------------------------|
           || NYSE     | ABC    | ["ABC"]        |    56.98 | 2021-09-13T04:42:44.812Z |
           || NASDAQ   | GE     | ["XYZ"]        |    83.13 | 2021-09-13T04:42:44.812Z |
           || OTCBB    | GMTQ   | ["ABC", "XYZ"] |   0.1111 | 2021-09-13T04:42:44.812Z |
           ||--------------------------------------------------------------------------|
           |""".stripMargin)
      assert(model == TableLiteral(
        columns = List(
          TableColumn(name = "exchange", `type` = StringType(6)),
          TableColumn(name = "symbol", `type` = StringType(4)),
          TableColumn(name = "codes", `type` = ArrayType(StringType(3), capacity = Some(2))),
          TableColumn(name = "lastSale", `type` = Float64Type),
          TableColumn(name = "lastSaleTime", `type` = DateTimeType)),
        value = List(
          List("NYSE".v, "ABC".v, ArrayLiteral("ABC".v), 56.98.v, DateHelper("2021-09-13T04:42:44.812Z").v),
          List("NASDAQ".v, "GE".v, ArrayLiteral("XYZ".v), 83.13.v, DateHelper("2021-09-13T04:42:44.812Z").v),
          List("OTCBB".v, "GMTQ".v, ArrayLiteral("ABC".v, "XYZ".v), 0.1111.v, DateHelper("2021-09-13T04:42:44.812Z").v)
        )))
    }

    it("should support Table literals with simple types") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|from (
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | QED    | NASDAQ   |          |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    | ABC    | OTCBB    |    5.887 |
           |    |------------------------------|
           |) where lastSale < 75 order by lastSale
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12)
      ))
    }

    it("should support Table literals with extraneous characters") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """||------------------------------------------------|
           || name            | symbol | exchange | lastSale |
           ||------------------------------------------------|
           || ABC & Co        | ABC    | AMEX     |    11.46 |
           || ACME Inc.       | ACME   | NYSE     |    56.78 |
           || BFG Corp.       | BFG    | NYSE     |   113.56 |
           || GreedIsGood.com | GREED  | NASDAQ   |  2345.78 |
           ||------------------------------------------------|
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("name" -> "ABC & Co", "symbol" -> "ABC", "exchange" -> "AMEX", "lastSale" -> 11.46),
        Map("name" -> "ACME Inc.", "symbol" -> "ACME", "exchange" -> "NYSE", "lastSale" -> 56.78),
        Map("name" -> "BFG Corp.", "symbol" -> "BFG", "exchange" -> "NYSE", "lastSale" -> 113.56),
        Map("name" -> "GreedIsGood.com", "symbol" -> "GREED", "exchange" -> "NASDAQ", "lastSale" -> 2345.78)
      ))
    }

    it("should support Table literals with dates and booleans") {
      val model = compiler.compile(
        """||----------------------------------------------------------------------------------------------------------------------------------|
           || name        | canonicalPath                                | lastModified             | length | isDirectory | isFile | isHidden |
           ||----------------------------------------------------------------------------------------------------------------------------------|
           || kungfu      | /Us/ldaniels/GitHub/lollypop/app/kungfu      | 2023-05-23T23:55:34.869Z |    160 | true        | false  | false    |
           || core        | /Us/ldaniels/GitHub/lollypop/app/core        | 2023-05-23T21:20:11.818Z |    160 | true        | false  | false    |
           || jdbc-driver | /Us/ldaniels/GitHub/lollypop/app/jdbc-driver | 2023-06-29T22:26:20.960Z |    160 | true        | false  | false    |
           ||----------------------------------------------------------------------------------------------------------------------------------|
           |""".stripMargin)
      assert(model == TableLiteral(
        columns = List(
          TableColumn(name = "name", `type` = StringType(11)),
          TableColumn(name = "canonicalPath", `type` = StringType(44)),
          TableColumn(name = "lastModified", `type` = DateTimeType),
          TableColumn(name = "length", `type` = Int64Type),
          TableColumn(name = "isDirectory", `type` = BooleanType),
          TableColumn(name = "isFile", `type` = BooleanType),
          TableColumn(name = "isHidden", `type` = BooleanType),
        ),
        value = List(
          List("kungfu".v, "/Us/ldaniels/GitHub/lollypop/app/kungfu".v, DateHelper("2023-05-23T23:55:34.869Z").v, 160L.v, true.v, false.v, false.v),
          List("core".v, "/Us/ldaniels/GitHub/lollypop/app/core".v, DateHelper("2023-05-23T21:20:11.818Z").v, 160L.v, true.v, false.v, false.v),
          List("jdbc-driver".v, "/Us/ldaniels/GitHub/lollypop/app/jdbc-driver".v, DateHelper("2023-06-29T22:26:20.960Z").v, 160L.v, true.v, false.v, false.v),
        )))
    }

  }

}
