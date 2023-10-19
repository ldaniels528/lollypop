package com.qwery.runtime.instructions.queryables

import com.qwery.language.models.@@@
import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.runtime.datatypes.{Int32Type, StringType}
import com.qwery.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.qwery.runtime.devices.TableColumn
import com.qwery.runtime.instructions.expressions.{Dictionary, Graph, GraphResult}
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class GraphTest extends AnyFunSpec {

  describe(classOf[Graph].getSimpleName) {

    it("should compile a Graph model") {
      val model = QweryCompiler().compile(
        """|graph { "shape": "pie" } from @@exposure
           |""".stripMargin)
      assert(model == Graph(chart = Dictionary("shape" -> "pie".v), from = From(@@@("exposure"))))
    }

    it("should decompile a Graph model") {
      val model = Graph(chart = Dictionary("shape" -> "pie".v), from = From("exposure".f))
      assert(model.toSQL == """graph { shape: "pie" } from exposure""")
    }

    it("should produce a Drawing result") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|declare table exposure(exchange: String(6), total: Int)
           |  containing values ("NASDAQ", 1276), ("AMEX", 1259), ("NYSE", 1275), ("OTCBB", 1190)
           |graph { shape: "pie" } from @@exposure
           |""".stripMargin)
      val expected = GraphResult(
        options = Map("shape" -> "pie"),
        data = createQueryResultTable(
          columns = List(
            TableColumn(name = "exchange", `type` = StringType(6)),
            TableColumn(name = "total", `type` = Int32Type)),
          data = List(
            Map("exchange" -> "NASDAQ", "total" -> 1276),
            Map("exchange" -> "AMEX", "total" -> 1259),
            Map("exchange" -> "NYSE", "total" -> 1275),
            Map("exchange" -> "OTCBB", "total" -> 1190))
        ))
      val drawingResult = Option(result).collect { case dr: GraphResult => dr }
      assert(drawingResult.map(_.options) contains expected.options)
      assert(drawingResult.map(_.data.columns) contains expected.data.columns)
      assert(drawingResult.toList.flatMap(_.data.toMapGraph) == expected.data.toMapGraph)
    }

  }

}
