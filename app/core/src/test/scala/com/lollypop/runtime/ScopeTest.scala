package com.lollypop.runtime

import com.lollypop.AppConstants
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.models.Operation.RichOperation
import com.lollypop.language.models._
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.datatypes.{Float64Type, Int32Type, Int64Type, StringType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.queryables.{From, This}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Scope Test Suite
 */
class ScopeTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[Scope].getSimpleName) {

    it("should override the database from a parent scope") {
      val (parentScope, _, _) = LollypopVM.executeSQL(Scope(), "namespace 'slovett.work'")
      val (childScope, _, _) = LollypopVM.executeSQL(Scope(parentScope = parentScope), "namespace 'ldaniels.work'")
      assert(childScope.getDatabase contains "ldaniels")
      assert(childScope.getSchema contains "work")
    }

    it("should provide the row ID of the current row") {
      val scope = Scope().withCurrentRow(Some(Row(id = 1001, metadata = RowMetadata(), columns = Nil, fields = Nil)))
      assert(scope.resolve("__id") contains 1001)
    }

    it("should provide the row ID of the current row from a parent scope") {
      val parentScope = Scope().withCurrentRow(Some(Row(id = 1001, metadata = RowMetadata(), columns = Nil, fields = Nil)))
      val childScope = Scope(parentScope)
      assert(childScope.resolve("__id") contains 1001)
    }

    it("should override the row ID of the current row from a parent scope") {
      val parentScope = Scope().withCurrentRow(Some(Row(id = 1001, metadata = RowMetadata(), columns = Nil, fields = Nil)))
      val childScope = Scope(parentScope = parentScope).withCurrentRow(Some(Row(id = 1002, metadata = RowMetadata(), columns = Nil, fields = Nil)))
      assert(childScope.resolve("__id") contains 1002)
    }

    it("should provide contents for fields of the current row") {
      val columns = List(
        TableColumn(name = "symbol", `type` = StringType(5), defaultValue = Some("ABC".v)),
        TableColumn(name = "lastSale", `type` = Float64Type, defaultValue = Some(18.22.v)))
      val scope0 = Scope()
      val scope = scope0
        .withCurrentRow(Some(Row(id = 1001, metadata = RowMetadata(), columns = columns, fields = columns.map(c =>
          Field(name = c.name, metadata = FieldMetadata(), value =
            c.defaultValue.flatMap(v => Option(LollypopVM.execute(scope0, v)._3)).map(c.`type`.convert))
        ))))
      assert(scope.resolve("__id") contains 1001)
      assert(scope.resolve("symbol") contains "ABC")
      assert(scope.resolve("lastSale") contains 18.22)
    }

    it("should distinguish between variables and fields of the current row") {
      val columns = List(
        TableColumn(name = "symbol", `type` = StringType(5), defaultValue = Some("ABC".v)),
        TableColumn(name = "lastSale", `type` = Float64Type, defaultValue = Some(18.22.v)))
      val scope0 = Scope()
      val scope = scope0
        .withVariable(name = "symbol", value = Some("BROK"))
        .withCurrentRow(Some(Row(id = 1001, metadata = RowMetadata(), columns = columns, fields = columns.map(c =>
          Field(name = c.name, metadata = FieldMetadata(), value = c.defaultValue.flatMap(v => Option(LollypopVM.execute(scope0, v)._3)).map(c.`type`.convert))
        ))))
      assert(LollypopVM.executeSQL(scope, "symbol")._3 == "ABC")
      assert(LollypopVM.executeSQL(scope, "$symbol")._3 == "BROK")
    }

    it("should create and read variables") {
      val scope = Scope().withVariable(name = "x", `type` = Int32Type, value = Some(5), isReadOnly = false)
      assert(scope.resolve("x") contains 5)
    }

    it("should provide the state for expressions") {
      import com.lollypop.language.models.Expression.implicits._
      val parentScope = Scope().withVariable(name = "x", `type` = Int32Type, value = Some(5), isReadOnly = false)
      val scope = Scope(parentScope).withVariable(name = "y", `type` = Int32Type, value = Some(2), isReadOnly = false)
      val expr: Expression = "x".f + "y".f + 7
      val (_, _, result) = LollypopVM.execute(scope, expr)
      assert(result == 14)
    }

    it("should read fields from a row") {
      implicit val structure: RecordStructure = RecordStructure(Seq(
        TableColumn(name = "symbol", `type` = StringType(5)),
        TableColumn(name = "exchange", `type` = StringType(6))
      ))

      val scope = Scope().withCurrentRow(Some(Map("symbol" -> "ABC", "exchange" -> "OTCBB").toRow(rowID = 100)))
      assert(scope.resolve("__id") contains 100L)
      assert(scope.resolve("symbol") contains "ABC")
      assert(scope.resolve("exchange") contains "OTCBB")
    }

    it("should query the `this` variable") {
      implicit val structure: RecordStructure = RecordStructure(Seq(
        TableColumn(name = "symbol", `type` = StringType(5)),
        TableColumn(name = "exchange", `type` = StringType(6))
      ))

      val scope = Scope()
        .withVariable(name = "position", `type` = Int64Type, value = Some(1), isReadOnly = false)
        .withVariable(name = "index", `type` = Int32Type, value = Some(1), isReadOnly = false)
        .withCurrentRow(Some(Map("symbol" -> "ABC", "exchange" -> "OTCBB").toRow(rowID = 0)))

      val (_, _, device) = LollypopVM.search(scope, From(This()))
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph.filterNot(_.exists {
        case ("name", "π") => true
        case ("name", "stdout") => true
        case ("name", "stderr") => true
        case ("name", "stdin") => true
        case ("name", "Node") => true
        case ("name", "OS") => true
        case ("name", "Random") => true
        case _ => false
      }).toSet == Set(
        Map("name" -> "position", "value" -> "1", "kind" -> "Long"),
        Map("name" -> "__id", "value" -> "0", "kind" -> "Long"),
        Map("name" -> "symbol", "value" -> "\"ABC\"", "kind" -> "String"),
        Map("name" -> "index", "value" -> "1", "kind" -> "Integer"),
        Map("name" -> "exchange", "value" -> "\"OTCBB\"", "kind" -> "String")
      ))
    }

    it("should read the database from a parent scope") {
      val (parentScope, _, _) = LollypopVM.executeSQL(Scope(), "namespace 'ldaniels.work'")
      val childScope = Scope(parentScope)
      assert(childScope.getDatabase contains "ldaniels")
      assert(childScope.getSchema contains "work")
    }

    it("should update the name upon realizing an object reference from a parent scope") {
      val (parentScope, _, _) = LollypopVM.executeSQL(Scope(), "namespace `securities.stocks`")
      val childScope = Scope(parentScope)
      assert(DatabaseObjectRef("nasdaq").realize(childScope) == DatabaseObjectNS(databaseName = "lollypop", schemaName = "public", name = "nasdaq"))
    }

    it("should read variables from a parent scope") {
      val parentScope = Scope().withVariable(name = "x", `type` = Int32Type, value = Some(5), isReadOnly = true)
      val childScope = Scope(parentScope).withVariable(name = "y", `type` = Int32Type, value = Some(7), isReadOnly = false)
      assert(parentScope.resolve("x") contains 5)
      assert(parentScope.getVariable("y").isEmpty)
      assert(childScope.resolve("y") contains 7)
      assert(childScope.resolve("x") contains 5)
    }

    it("should read table variables from a parent scope") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|declare table travelers(lastName String(12), firstName String(12), destAirportCode String(3))
            |{
            |   insert into @travelers (lastName, firstName, destAirportCode)
            |   values ('JONES', 'GARRY', 'SNA'), ('JONES', 'DEBBIE', 'SNA'),
            |          ('JONES', 'TAMERA', 'SNA'), ('JONES', 'ERIC', 'SNA'),
            |          ('ADAMS', 'KAREN', 'DTW'), ('ADAMS', 'MIKE', 'DTW'),
            |          ('JONES', 'SAMANTHA', 'BUR'), ('SHARMA', 'PANKAJ', 'LAX')
            |}
            |travelers
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("destAirportCode" -> "SNA", "lastName" -> "JONES", "firstName" -> "GARRY"),
        Map("destAirportCode" -> "SNA", "lastName" -> "JONES", "firstName" -> "DEBBIE"),
        Map("destAirportCode" -> "SNA", "lastName" -> "JONES", "firstName" -> "TAMERA"),
        Map("destAirportCode" -> "SNA", "lastName" -> "JONES", "firstName" -> "ERIC"),
        Map("destAirportCode" -> "DTW", "lastName" -> "ADAMS", "firstName" -> "KAREN"),
        Map("destAirportCode" -> "DTW", "lastName" -> "ADAMS", "firstName" -> "MIKE"),
        Map("destAirportCode" -> "BUR", "lastName" -> "JONES", "firstName" -> "SAMANTHA"),
        Map("destAirportCode" -> "LAX", "lastName" -> "SHARMA", "firstName" -> "PANKAJ")
      ))
    }

    it("should properly handle scoping of functions") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        s"""|var isAlive = true
            |def main() := {
            |   var x: Int = 0
            |   var y: Int = 0
            |   while isAlive is true {
            |       x += 1
            |       y += 5
            |       if x > 5 isAlive = false
            |   }
            |   y
            |}
            |main()
            |""".stripMargin)
      assert(result == 30)
    }

    it("should updates of variables in the parent from the child scope") {
      val (scope, _, result) = LollypopVM.executeSQL(Scope(),
        s"""|var x: Int = 3
            |{
            |   x += 2
            |   var y: Int  = 7
            |   x + y
            |}
            |""".stripMargin)
      assert(scope.resolve("x") contains 5)
      assert(scope.resolve("y").isEmpty)
      assert(result == 12)
    }

    it("should commit updates of variables in the parent from the child scope") {
      val (scope, _, result) = LollypopVM.executeSQL(Scope(),
        s"""|var x: Int = 3
            |var y: Int = 1
            |val a: Int = {
            |   var z: Int = 7
            |   x += 2
            |   y += 1
            |   x + y + z
            |}
            |a
            |""".stripMargin)
      info(s"scope => ${scope.getClass.getSimpleName}")
      assert(scope.resolve("x") contains 5)
      assert(scope.resolve("y") contains 2)
      assert(scope.resolve("z").isEmpty)
      assert(scope.resolve("a") contains 14)
      assert(result == 14)
    }

    it("should commit updates of variables in the parent from the multiple child scopes") {
      val (scope, _, result) = LollypopVM.executeSQL(Scope(),
        s"""|var x: Int = 3
            |var y: Int = 1
            |{
            |   x += 2
            |   y += 1
            |   {
            |     var z: Int = 7
            |     x + y + z
            |   }
            |}
            |""".stripMargin)
      assert(scope.resolve("x") contains 5)
      assert(scope.resolve("y") contains 2)
      assert(scope.resolve("z").isEmpty)
      assert(result == 14.0)
    }

    it("should execute methods of Scala-native implicit classes") {
      val scope = Scope().importImplicitClass(implicitClass = Class.forName("com.lollypop.util.StringRenderHelper$StringRenderer"))
      val result = scope.invokeImplicitMethod("Hello", "renderAsJson")
      assert(result == "\"Hello\"")
    }

    it("should provide a collection of import implicit classes and methods") {
      val scope = Scope().importImplicitClass(implicitClass = Class.forName("com.lollypop.util.StringRenderHelper$StringRenderer"))
      val result = scope.resolve("__implicit_imports__").orNull
      assert(result == Map(
        "com.lollypop.util.StringRenderHelper$StringRenderer" -> Set(
          "_render$default$2", "renderPretty", "renderFriendly", "renderAsJson", "renderTable", "render", "_render",
          "escapeString", "collapse", "item"
        )))
    }

    it("should provide the Lollypop version string") {
      val version = Scope().resolve("__version__").orNull
      assert(version == AppConstants.version)
    }

  }

}
