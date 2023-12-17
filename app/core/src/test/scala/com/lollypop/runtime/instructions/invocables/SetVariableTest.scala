package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.implicits._
import com.lollypop.language.models.OrderColumn
import com.lollypop.language.{TokenStream, _}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.conditions.EQ
import com.lollypop.runtime.instructions.invocables.SetVariableTest.{AA, BB, CC, java_util_Map}
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

import java.util
import scala.collection.mutable

class SetVariableTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[SetVariable].getSimpleName) {

    it("should compile scalar variables") {
      val results = compiler.compile(
        """|set message = 'Hello {{ name }}'
           |""".stripMargin)
      assert(results == SetAnyVariable("message".f, "Hello {{ name }}"))
    }

    it("should decompile set scalar variable") {
      val model = ScopeModificationBlock(SetAnyVariable("x".f, 3.v), SetAnyVariable("y".f, 6.v), SetAnyVariable("z".f, 9.v))
      assert(model == compiler.compile("set x = 3, y = 6, z = 9"))
    }

    it("should compile table variables") {
      val results = compiler.nextOpCodeOrDie(TokenStream(
        """|set securities = (
           |  select Symbol, Name, Sector, Industry, `Summary Quote`
           |  from Securities
           |  where Industry == 'Oil/Gas Transmission'
           |  order by Symbol asc
           |)
           |""".stripMargin))
      assert(results == SetAnyVariable(ref = "securities".f,
        Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "Summary Quote".f),
          from = Some(DatabaseObjectRef("Securities")),
          where = Some(EQ("Industry".f, "Oil/Gas Transmission".v)),
          orderBy = List(OrderColumn("Symbol", isAscending = true)))
      ))
    }

    it("should execute: x.a.b.c = 6544 (Product)") {
      val scope = Scope().withVariable(name = "x", value = AA(a = BB(b = CC(c = 1111))))
      val (_, _, result) = LollypopVM.executeSQL(scope,
        """|x.a.b.c = 6544
           |x.a.b.c
           |""".stripMargin)
      assert(result == 6544)
    }

    it("should execute: x.a.b.c = 6544 (mutable.Map)") {
      def MMap(values: (String, Any)*): mutable.Map[String, Any] = mutable.Map[String, Any](values: _*)
      val scope = Scope().withVariable(name = "x", value = MMap("a" -> MMap("b" -> MMap("c" -> 1111))))
      val (_, _, result) = LollypopVM.executeSQL(scope,
        """|x.a.b.c = 6544
           |x.a.b.c
           |""".stripMargin)
      assert(result == 6544)
    }

    it("should execute: x.a.b.c = 6544 (java.util.Map)") {
      val scope = Scope().withVariable(name = "x", value = java_util_Map("a" -> java_util_Map("b" -> java_util_Map("c" -> 1111))))
      val (_, _, result) = LollypopVM.executeSQL(scope,
        """|x.a.b.c = 6544
           |x.a.b.c
           |""".stripMargin)
      assert(result == 6544)
    }

    it("should execute: 'x = { a: { b: { c : 98 } } }' then 'x.a.b.c = 255'") {
      val (scopeA, _, resultA) = LollypopVM.executeSQL(Scope(),
        """|val x = { a: { b: { c : 98 } } }
           |x.a.b.c
           |""".stripMargin)
      assert(resultA == 98)

      val (_, _, resultB) = LollypopVM.executeSQL(scopeA,
        """|x.a.b.c = 255
           |x.a.b.c
           |""".stripMargin)
      assert(resultB == 255)
    }

  }

}

object SetVariableTest {

  def java_util_Map(values: (String, Any)*): util.Map[String, Any] = {
    val m = new util.HashMap[String, Any]()
    values.foreach { case (k, v) => m.put(k, v) }
    m
  }

  case class AA(a: BB)

  case class BB(b: CC)

  case class CC(var c: Any)

}
