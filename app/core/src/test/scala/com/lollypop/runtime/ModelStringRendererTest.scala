package com.lollypop.runtime

import com.lollypop.runtime.ModelStringRenderer.ModelStringRendering
import com.lollypop.runtime.ModelStringRendererTest.Student
import org.scalatest.funspec.AnyFunSpec

class ModelStringRendererTest extends AnyFunSpec {
  private val compiler = LollypopCompiler()

  describe(classOf[LollypopCompiler].getSimpleName) {

    it("""should render: Student(id="5b9e00d5-fafd-47f2-bab9-557dabf81f16", name="Tom Hanks", age=61)""") {
      val student = Student(id = "5b9e00d5-fafd-47f2-bab9-557dabf81f16", name = "Tom Hanks", age = 61)
      assert(student.asModelString ==
        """|Student(id="5b9e00d5-fafd-47f2-bab9-557dabf81f16", name="Tom Hanks", age=61)
           |""".stripMargin.trim)
    }

    it("""should render: def ¡(n: Double) := iff(n <= 1.0, 1.0, n * ¡(n - 1.0))""") {
      val model = compiler.compile(
        """|def ¡(n: Double) := iff(n <= 1.0, 1.0, n * ¡(n - 1.0))
           |""".stripMargin)
      info(model.asModelString)
      assert(model.asModelString ==
        """|Def(function=NamedFunction(name="¡", params=List(Column(name="n", type="Double".ct, defaultValue=None, isRowID=false)), code=Iff(condition=LTE(a="n".f, b=1.0.v), trueValue=1.0.v, falseValue=Times(a="n".f, b="¡".fx(Minus(a="n".f, b=1.0.v)))), returnType_?=None))
           |""".stripMargin.trim)
    }

    it("""should render: select avgLastSale: avg(lastSale) from @stocks""") {
      val model = compiler.compile(
        """|select avgLastSale: avg(lastSale) from @stocks
           |""".stripMargin)
      info(model.asModelString)
      assert(model.asModelString ==
        """|Select(fields=List(Avg(expression="lastSale".f)), from=Some(@@("stocks")), joins=Nil, groupBy=Nil, having=None, orderBy=Nil, where=None, limit=None)
           |""".stripMargin.trim)
    }

    it("""should render: transpose(help('select'))""") {
      val model = compiler.compile(
        """|transpose(help('select'))
           |""".stripMargin)
      info(model.asModelString)
      assert(model.asModelString ==
        """|Transpose(expression=Help(patternExpr=Some("select".v)))
           |""".stripMargin.trim)
    }

    it("""should render: p3d([ x: 123, y:13, z: 67 ])""") {
      val model = compiler.compile(
        """|p3d([ x: 123, y:13, z: 67 ])
           |""".stripMargin)
      info(model.asModelString)
      assert(model.asModelString ==
        """|"p3d".fx(ArrayLiteral(123.v, 13.v, 67.v))
           |""".stripMargin.trim)
    }

    it("""should render: drawPoint($x, $y, $z)""") {
      val model = compiler.compile(
        """|drawPoint($x, $y, $z)
           |""".stripMargin)
      info(model.asModelString)
      assert(model.asModelString ==
        """|"drawPoint".fx($("x"), $("y"), $("z"))
           |""".stripMargin.trim)
    }

    it("""should render: response = { 'message1' : 'Hello World' }""") {
      val model = compiler.compile(
        """|response = { 'message1' : 'Hello World' }
           |""".stripMargin)
      info(model.asModelString)
      assert(model.asModelString ==
        """|SetVariableExpression(ref="response".f, expression=Dictionary(("message1","Hello World".v)))
           |""".stripMargin.trim)
    }

  }

}

object ModelStringRendererTest {

  case class Student(id: String, name: String, age: Int)

}