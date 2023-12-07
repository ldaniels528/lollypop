package com.lollypop.language

import com.lollypop.language.models._
import com.lollypop.runtime.instructions.conditions._
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec

/**
 * Template Processor Test
 * @author lawrence.daniels@gmail.com
 */
class TemplateProcessorTest extends AnyFunSpec {
  private implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[TokenStreamExtensions].getSimpleName) {

    it("should resolve as a function call") {
      val ts = TokenStream("""f()""")
      assert(ts.isFunctionCall)
    }

    it("should not resolve as a function call") {
      val ts = TokenStream(
        """|f
           |()""".stripMargin)
      assert(!ts.isFunctionCall)
    }

  }

  describe(classOf[TemplateProcessor].getSimpleName) {
    import com.lollypop.language.implicits._

    it("should parse argument tags (%A)") {
      verify(text = "(1,2,3)", template = "%A:args")(SQLTemplateParams(expressionLists = Map("args" -> List(1, 2, 3))))
    }

    it("should parse atom tags (%a)") {
      verify(text = "Customers", template = "%a:target")(SQLTemplateParams(atoms = Map("target" -> "Customers")))
    }

    it("should parse chooser tags (%C)") {
      verify(text = "into", template = "%C(mode|into|overwrite)")(SQLTemplateParams(atoms = Map("mode" -> "into")))
      verify(text = "overwrite", template = "%C(mode|into|overwrite)")(SQLTemplateParams(atoms = Map("mode" -> "overwrite")))
    }

    it("should parse condition tags (%c)") {
      verify(text = "custId == 123", template = "%c:condition")(SQLTemplateParams(instructions = Map("condition" -> (FieldRef("custId") === 123d))))
    }

    it("should parse expression tags (%E)") {
      verify(text = "field1, 'hello', 5", template = "%E:fields")(SQLTemplateParams(expressionLists = Map("fields" -> List("field1".f, "hello", 5.0))))
    }

    it("should parse assignable expression tags (%e)") {
      verify(text = "(x + 1) * 2", template = "%e:expression")(SQLTemplateParams(instructions = Map("expression" -> (("x".f + 1) * 2))))
    }

    it("should parse field tags (%F)") {
      verify(text = "field1, field2, field3", template = "%F:fields")(SQLTemplateParams(fieldLists = Map("fields" -> List("field1".f, "field2".f, "field3".f))))
    }

    it("should parse infrastructure type tags (%I)") {
      verify(text = "external table", template = "%I:type")(SQLTemplateParams(atoms = Map("type" -> "external table")))
      verify(text = "view", template = "%I:type")(SQLTemplateParams(atoms = Map("type" -> "view")))
    }

    it("should parse Java instance field expressions") {
      verify(text = "myValue->value", template = "%a:variable -> %a:fieldName")(SQLTemplateParams(
        atoms = Map("variable" -> "myValue", "fieldName" -> "value"),
        keywords = Set("->")
      ))
    }

    it("should parse Java instance method expressions") {
      verify(text = "offScreen->setColor(color)", template = "%a:variable -> %a:methodName ( ?%E:args )")(SQLTemplateParams(
        atoms = Map("variable" -> "offScreen", "methodName" -> "setColor"),
        expressionLists = Map("args" -> List("color".f)),
        keywords = Set("->", "(", ")")
      ))
    }

    it("should parse Java static field expressions") {
      verify(text = "`java.awt.Color`->BLACK", template = "%a:className -> %a:fieldName")(SQLTemplateParams(
        atoms = Map("className" -> "java.awt.Color", "fieldName" -> "BLACK"),
        keywords = Set("->")
      ))
    }

    it("should parse Java static method expressions") {
      verify(text = "`org.jsoup.Jsoup`->parse(file, 'UTF-8')", template = "%a:className -> %a:methodName ( ?%E:args )")(SQLTemplateParams(
        atoms = Map("className" -> "org.jsoup.Jsoup", "methodName" -> "parse"),
        expressionLists = Map("args" -> List("file".f, "UTF-8")),
        keywords = Set("->", "(", ")")
      ))
    }

    it("should parse keyword tags") {
      verify(text = "LOCATION", template = "LOCATION")(SQLTemplateParams(keywords = Set("LOCATION")))
    }

    it("should parse location tags (%L)") {
      verify(text = "assets", template = "%L:table")(SQLTemplateParams(locations = Map("table" -> DatabaseObjectRef("assets"))))
      verify(text = "@assets", template = "%L:table")(SQLTemplateParams(locations = Map("table" -> @@("assets"))))
      verify(text = "`the assets`", template = "%L:table")(SQLTemplateParams(locations = Map("table" -> DatabaseObjectRef("the assets"))))
    }

    it("should parse location tags (%M)") {
      verify(text = "enabled is true", template = "%M:%c,%q:cond")(SQLTemplateParams(instructions = Map("cond" -> Is("enabled".f, true.v))))
    }

    it("should parse ordered field tags (%o)") {
      verify(text = "field1 desc, field2 asc", template = "%o:orderedFields")(SQLTemplateParams(orderedFields = Map(
        "orderedFields" -> List("field1".desc, "field2".asc)
      )))
    }

    it("should parse parameter tags (%P)") {
      verify(text =
        """|name String(128),
           |address String(128)[2],
           |addressType Enum ( house, condo, land, multiFamily ),
           |comments Table ( remarks String(255), createdDate DateTime )[5],
           |age Int,
           |dob DateTime
           |""".stripMargin, template = "%P:params") {
        SQLTemplateParams(parameters = Map(
          "params" -> List(
            Column(name = "name", `type` = ColumnType("String", size = 128)),
            Column(name = "address", `type` = ColumnType.array(columnType = ColumnType("String", size = 128), arraySize = 2)),
            Column(name = "addressType", `type` = ColumnType.`enum`(enumValues = Seq("house", "condo", "land", "multiFamily"))),
            Column(name = "comments", `type` = ColumnType.table(capacity = 5, columns = Seq(
              Column(name = "remarks", `type` = ColumnType("String", size = 255)),
              Column(name = "createdDate", `type` = ColumnType("DateTime"))
            ))),
            Column(name = "age", `type` = ColumnType("Int")),
            Column(name = "dob", `type` = ColumnType("DateTime")))
        ))
      }
    }

    it("should parse direct query tags (%Q) - @variable") {
      verify(text = "@addressBook", template = "%Q:variable")(SQLTemplateParams(instructions = Map(
        "variable" -> @@("addressBook")
      )))
    }

    it("should parse regular expression tags (%r)") {
      verify(text = "'123ABC'", template = "%r`\\d{3,4}\\S+`")(SQLTemplateParams())
    }

    it("should parse variable reference tags (%v)") {
      verify(text = "variable", template = "%a:ref")(SQLTemplateParams(atoms = Map("ref" -> "variable")))
    }

    it("should parse optionally required tags (?, +?)") {
      verify(text = "limit 100", template = "?limit +?%e:limit")(SQLTemplateParams(instructions = Map("limit" -> Literal(100)), keywords = Set("limit")))
      verify(text = "limit AAA", template = "?limit +?%e:limit")(SQLTemplateParams(instructions = Map("limit" -> "AAA".f), keywords = Set("limit")))
    }

    it("should process CAST(1234 to String)") {
      verify(text = "CAST(1234 to String)", template = "CAST ( %e:value to %T:type )")(SQLTemplateParams(
        instructions = Map("value" -> Literal(1234)),
        types = Map("type" -> ColumnType("String")),
        keywords = Set("CAST", "(", "to", ")")
      ))
    }

    it("should process IF(LastSale < 1, 'Penny Stock', 'Stock')") {
      verify(text = "IF(LastSale < 1, 'Penny Stock', 'Stock')", template = "IF ( %c:condition , %e:trueValue , %e:falseValue )")(SQLTemplateParams(
        instructions = Map("condition" -> ("LastSale".f < 1), "trueValue" -> "Penny Stock", "falseValue" -> "Stock"),
        keywords = Set("IF", ",", "(", ")")
      ))
    }

    it("should process PRINT(deptCode)") {
      verify(text = "PRINT(deptCode)", template = "PRINT ( %a:variable )")(SQLTemplateParams(
        atoms = Map("variable" -> "deptCode"),
        keywords = Set("PRINT", "(", ")")
      ))
    }

    it("should process SUBSTRING('Hello World', 5, 1)") {
      verify(text = "SUBSTRING('Hello World', 5, 1)", template = "SUBSTRING ( %E:values )")(SQLTemplateParams(
        expressionLists = Map("values" -> List[Expression]("Hello World", 5, 1)),
        keywords = Set("SUBSTRING", "(", ")")
      ))
    }

    it("should process sum(deptCode)") {
      verify("sum(deptCode)", "sum ( %f:field )")(SQLTemplateParams(
        instructions = Map("field" -> "deptCode".f),
        keywords = Set("sum", "(", ")")
      ))
    }

  }

  def verify(text: String, template: String)(expected: SQLTemplateParams): Assertion = {
    info(s"'$template' <~ '$text'")
    val actual = SQLTemplateParams(TokenStream(text), template)
    println(s"actual:   ${actual.parameters}")
    println(s"expected: ${expected.parameters}")
    assert(actual == expected, s"'$text' ~> '$template' failed")
  }

}
