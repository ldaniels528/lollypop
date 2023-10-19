package com.qwery.language

import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.language.models.{Column, ColumnType}
import com.qwery.runtime.QweryCompiler
import org.scalatest.funspec.AnyFunSpec

class ColumnTypeParserTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(ColumnTypeParser.getClass.getSimpleName) {

    it("should parse instruction: Char(20)") {
      val columnType = ColumnTypeParser.parseColumnType(TokenStream("Char(20)"))
      assert(columnType contains ColumnType(name = "Char", size = 20))
    }

    it("should parse: DateTime") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("DateTime"))
      assert(columnType == ColumnType(name = "DateTime"))
    }

    it("should parse: String(20)") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("String(20)"))
      assert(columnType == ColumnType(name = "String", size = 20))
    }

    it("should parse: Numeric(10, 2)") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Numeric(10, 2)"))
      assert(columnType == ColumnType(name = "Numeric", size = 10, precision = 2))
    }

    it("should parse: Byte[125]") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Byte[125]"))
      assert(columnType == ColumnType.array(columnType = "Byte".ct, arraySize = 125))
    }

    it("should parse: String(120)[125]") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("String(120)[125]"))
      assert(columnType == ColumnType.array(columnType = ColumnType(name = "String", size = 120), arraySize = 125))
    }

    it("should parse: Numeric(9, 4)[1000]") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Numeric(9, 4)[1000]"))
      assert(columnType == ColumnType.array(columnType = ColumnType(name = "Numeric", size = 9, precision = 4), arraySize = 1000))
    }

    it("should parse: Enum(ONE, TWO, THREE, FOUR)") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Enum(ONE, TWO, THREE, FOUR)"))
      assert(columnType == ColumnType.`enum`(enumValues = Seq("ONE", "TWO", "THREE", "FOUR")))
    }

    it("should parse: Enum(ONE, TWO, THREE, FOUR)[5]") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Enum(ONE, TWO, THREE, FOUR)[5]"))
      assert(columnType == ColumnType.array(columnType = ColumnType.`enum`(enumValues = Seq("ONE", "TWO", "THREE", "FOUR")), arraySize = 5))
    }

    it("should parse: String*") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("String*"))
      assert(columnType == ColumnType(name = "String").copy(isPointer = true))
    }

    it("should parse: String(80)*") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("String(80)*"))
      assert(columnType == ColumnType(name = "String", size = 80).copy(isPointer = true))
    }

    it("should parse: String(80)[12]*") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("String(80)[12]*"))
      assert(columnType == ColumnType.array(columnType = ColumnType(name = "String", size = 80), arraySize = 12).copy(isPointer = true))
    }

    it("should parse: Table(item String, price Double)") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Table(item String, price Double)"))
      assert(columnType.isMultiTenantTable)
      assert(columnType == ColumnType.table(columns = Seq(
        new Column(name = "item", `type` = "String".ct),
        new Column(name = "price", `type` = "Double".ct)
      )))
    }

    it("should parse: Table(item String, price Double)[25]") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Table(item String, price Double)[25]"))
      assert(columnType.isClusteredTable)
      assert(columnType == ColumnType.table(
        capacity = 25,
        columns = Seq(
          new Column(name = "item", `type` = "String".ct),
          new Column(name = "price", `type` = "Double".ct)
        )))
    }

    it("should parse: Table(item String, price Double)[25]*") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Table(item String, price Double)[25]*"))
      assert(columnType.isBlobTable)
      assert(columnType == ColumnType.table(
        capacity = 25,
        isPointer = true,
        columns = Seq(
          new Column(name = "item", `type` = "String".ct),
          new Column(name = "price", `type` = "Double".ct)
        )))
    }

    it("should parse: Table(item String, price Double)*") {
      val columnType = ColumnTypeParser.nextColumnType(TokenStream("Table(item String, price Double)*"))
      assert(columnType.isBlobTable)
      assert(columnType == ColumnType.table(
        isPointer = true,
        columns = Seq(
          new Column(name = "item", `type` = "String".ct),
          new Column(name = "price", `type` = "Double".ct)
        )))
    }

  }

}
