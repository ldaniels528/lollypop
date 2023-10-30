package com.lollypop.runtime.instructions.operators

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.Instruction
import com.lollypop.language.models.Operation.RichOperation
import com.lollypop.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec

class OperationTest extends AnyFunSpec {

  describe(classOf[Amp].getSimpleName) {
    it("should equate: z & 3")(assert(("z".f & 3.v) == Amp("z".f, 3.v)))
    it("should compile: 9 & 3")(compile("9 & 3", Amp(9.v, 3.v)))
    it("should decompile: 9 & 3")(decompile(Amp(9.v, 3.v), "9 & 3"))
    it("should evaluate: 9 & 3")(evaluate("9 & 3", 1))

    it("should equate: z &= 3")(assert(("z".f &= 3.v) == Amp("z".f, 3.v).doAndSet))
    it("should compile: x &= 2")(compile("x &= 2", Amp("x".f, 2.v).doAndSet))
    it("should decompile: x &= 2")(decompile(Amp("x".f, 2.v).doAndSet, "x &= 2"))
    it("should evaluate: x &= 2")(evaluate(
      """|var x = 9
         |x &= 3
         |x
         |""".stripMargin, 1))
  }

  describe(classOf[AmpAmp].getSimpleName) {
    it("should equate: z && 6")(assert(("z".f && 6.v) == AmpAmp("z".f, 6.v)))
    it("should compile: 5 && 6")(compile("5 && 6", AmpAmp(5.v, 6.v)))
    it("should decompile: 5 && 6")(decompile(AmpAmp(5.v, 6.v), "5 && 6"))

    it("should equate: x &&= 1")(assert(("x".f &&= 1.v) == AmpAmp("x".f, 1.v).doAndSet))
    it("should compile: y &&= 2")(compile("y &&= 2", AmpAmp("y".f, 2.v).doAndSet))
    it("should decompile: z &&= 3")(decompile(AmpAmp("z".f, 3.v).doAndSet, "z &&= 3"))
  }

  describe(classOf[Bar].getSimpleName) {
    it("should equate: z | 3")(assert(("z".f | 3.v) == Bar("z".f, 3.v)))
    it("should compile: 9 | 3")(compile("9 | 3", Bar(9.v, 3.v)))
    it("should decompile: 9 | 3")(decompile(Bar(9.v, 3.v), "9 | 3"))
    it("should evaluate: 9 | 3")(evaluate("9 | 3", 11))

    it("should equate: z |= 3")(assert(("z".f |= 3.v) == Bar("z".f, 3.v).doAndSet))
    it("should compile: x |= 2")(compile("x |= 2", Bar("x".f, 2.v).doAndSet))
    it("should decompile: x |= 2")(decompile(Bar("x".f, 2.v).doAndSet, "x |= 2"))
    it("should evaluate: x |= 2")(evaluate(
      """|var x = 9
         |x |= 3
         |x
         |""".stripMargin, 11))
  }

  describe(classOf[BarBar].getSimpleName) {
    it("should equate: z || 3")(assert(("z".f || 3.v) == BarBar("z".f, 3.v)))
    it("should compile: 9 || 3")(compile("9 || 3", BarBar(9.v, 3.v)))
    it("should decompile: 9 || 3")(decompile(BarBar(9.v, 3.v), "9 || 3"))

    it("should equate: z ||= 3")(assert(("z".f ||= 3.v) == BarBar("z".f, 3.v).doAndSet))
    it("should compile: x ||= 2")(compile("x ||= 2", BarBar("x".f, 2.v).doAndSet))
    it("should decompile: x ||= 2")(decompile(BarBar("x".f, 2.v).doAndSet, "x ||= 2"))
  }

  describe(classOf[ColonColon].getSimpleName) {
    it("should equate: z :: 3")(assert(("z".f :: 3.v) == ColonColon("z".f, 3.v)))
    it("should compile: 9 :: 3")(compile("9 :: 3", ColonColon(9.v, 3.v)))
    it("should decompile: 9 :: 3")(decompile(ColonColon(9.v, 3.v), "9 :: 3"))

    it("should equate: z ::= 3")(assert(("z".f ::= 3.v) == ColonColon("z".f, 3.v).doAndSet))
    it("should compile: x ::= 2")(compile("x ::= 2", ColonColon("x".f, 2.v).doAndSet))
    it("should decompile: x ::= 2")(decompile(ColonColon("x".f, 2.v).doAndSet, "x ::= 2"))
  }

  describe(classOf[ColonColonColon].getSimpleName) {
    it("should equate: z ::: 3")(assert(("z".f ::: 3.v) == ColonColonColon("z".f, 3.v)))
    it("should compile: 9 ::: 3")(compile("9 ::: 3", ColonColonColon(9.v, 3.v)))
    it("should decompile: 9 ::: 3")(decompile(ColonColonColon(9.v, 3.v), "9 ::: 3"))

    it("should equate: z :::= 3")(assert(("z".f :::= 3.v) == ColonColonColon("z".f, 3.v).doAndSet))
    it("should compile: x :::= 2")(compile("x :::= 2", ColonColonColon("x".f, 2.v).doAndSet))
    it("should decompile: x :::= 2")(decompile(ColonColonColon("x".f, 2.v).doAndSet, "x :::= 2"))
  }

  describe(classOf[Div].getSimpleName) {
    it("should equate: z / 3")(assert(("z".f / 3.v) == Div("z".f, 3.v)))
    it("should compile: 15 / 3")(compile("15 / 3", Div(15.v, 3.v)))
    it("should decompile: 15 / 3")(decompile(Div(15.v, 3.v), "15 / 3"))
    it("should evaluate: 15 / 3")(evaluate("15 / 3", 5))

    it("should equate: x /= 1")(assert(("x".f /= 1.v) == Div("x".f, 1.v).doAndSet))
    it("should compile: y /= 2")(compile("y /= 2", Div("y".f, 2.v).doAndSet))
    it("should decompile: z /= 3")(decompile(Div("z".f, 3.v).doAndSet, "z /= 3"))
  }

  describe(classOf[GreaterGreater].getSimpleName) {
    it("should equate: z >> 6")(assert(("z".f >> 6.v) == GreaterGreater("z".f, 6.v)))
    it("should compile: 5 >> 6")(compile("5 >> 6", GreaterGreater(5.v, 6.v)))
    it("should decompile: 5 >> 6")(decompile(GreaterGreater(5.v, 6.v), "5 >> 6"))
    it("should evaluate: 5 >> 6")(evaluate("320 >> 6", 5))

    it("should equate: z >>= 3")(assert(("z".f >>= 3.v) == GreaterGreater("z".f, 3.v).doAndSet))
    it("should compile: x >>= 2")(compile("x >>= 2", GreaterGreater("x".f, 2.v).doAndSet))
    it("should decompile: x >>= 2")(decompile(GreaterGreater("x".f, 2.v).doAndSet, "x >>= 2"))
    it("should evaluate: x >>= 2")(evaluate(
      """|var x = 50
         |x >>= 2
         |x
         |""".stripMargin, 12))
  }

  describe(classOf[GreaterGreaterGreater].getSimpleName) {
    it("should equate: z >>> 6")(assert(("z".f >>> 6.v) == GreaterGreaterGreater("z".f, 6.v)))
    it("should compile: 5 >>> 6")(compile("5 >>> 6", GreaterGreaterGreater(5.v, 6.v)))
    it("should decompile: 5 >>> 6")(decompile(GreaterGreaterGreater(5.v, 6.v), "5 >>> 6"))
    it("should evaluate: 5 >>> 6")(evaluate("320 >>> 6", 5))

    it("should equate: z >>>= 3")(assert(("z".f >>>= 3.v) == GreaterGreaterGreater("z".f, 3.v).doAndSet))
    it("should compile: x >>>= 2")(compile("x >>>= 2", GreaterGreaterGreater("x".f, 2.v).doAndSet))
    it("should decompile: x >>>= 2")(decompile(GreaterGreaterGreater("x".f, 2.v).doAndSet, "x >>>= 2"))
    it("should evaluate: x >>>= 2")(evaluate(
      """|var x = 50
         |x >>>= 2
         |x
         |""".stripMargin, 12))
  }

  describe(classOf[LessLess].getSimpleName) {
    it("should equate: z << 6")(assert(("z".f << 6.v) == LessLess("z".f, 6.v)))
    it("should compile: 5 << 6")(compile("5 << 6", LessLess(5.v, 6.v)))
    it("should decompile: 5 << 6")(decompile(LessLess(5.v, 6.v), "5 << 6"))
    it("should evaluate: 5 << 6")(evaluate("5 << 6", 320))

    it("should equate: z <<= 3")(assert(("z".f <<= 3.v) == LessLess("z".f, 3.v).doAndSet))
    it("should compile: x <<= 2")(compile("x <<= 2", LessLess("x".f, 2.v).doAndSet))
    it("should decompile: x <<= 2")(decompile(LessLess("x".f, 2.v).doAndSet, "x <<= 2"))
    it("should evaluate: x <<= 2")(evaluate(
      """|var x = 50
         |x <<= 2
         |x
         |""".stripMargin, 200))
  }

  describe(classOf[LessLessLess].getSimpleName) {
    it("should equate: z <<< 6")(assert(("z".f <<< 6.v) == LessLessLess("z".f, 6.v)))
    it("should compile: 5 <<< 6")(compile("5 <<< 6", LessLessLess(5.v, 6.v)))
    it("should decompile: 5 <<< 6")(decompile(LessLessLess(5.v, 6.v), "5 <<< 6"))
    it("should evaluate: 5 <<< 6")(evaluate("5 <<< 6", 320))

    it("should equate: z <<<= 3")(assert(("z".f <<<= 3.v) == LessLessLess("z".f, 3.v).doAndSet))
    it("should compile: x <<<= 2")(compile("x <<<= 2", LessLessLess("x".f, 2.v).doAndSet))
    it("should decompile: x <<<= 2")(decompile(LessLessLess("x".f, 2.v).doAndSet, "x <<<= 2"))
    it("should evaluate: x <<<= 2")(evaluate(
      """|var x = 50
         |x <<<= 2
         |x
         |""".stripMargin, 200))
  }

  describe(classOf[Minus].getSimpleName) {
    it("should equate: z - 6")(assert(("z".f - 6.v) == Minus("z".f, 6.v)))
    it("should compile: 5 - 6")(compile("5 - 6", Minus(5.v, 6.v)))
    it("should decompile: 5 - 6")(decompile(Minus(5.v, 6.v), "5 - 6"))
    it("should evaluate: 5 - 6")(evaluate("5 - 6", -1))

    it("should equate: z -= 4")(assert(("z".f -= 4.v) == Minus("z".f, 4.v).doAndSet))
    it("should compile: x -= 10")(compile("x -= 10", Minus("x".f, 10.v).doAndSet))
    it("should decompile: x -= 10")(decompile(Minus("x".f, 10.v).doAndSet, "x -= 10"))
    it("should evaluate: x -= 10")(evaluate(
      """|var x = 100
         |x -= 10
         |x
         |""".stripMargin, 90))
  }

  describe(classOf[MinusMinus].getSimpleName) {
    it("should equate: z -- 6")(assert(("z".f -- 6.v) == MinusMinus("z".f, 6.v)))
    it("should compile: 5 -- 6")(compile("5 -- 6", MinusMinus(5.v, 6.v)))
    it("should decompile: 5 -- 6")(decompile(MinusMinus(5.v, 6.v), "5 -- 6"))

    it("should compile: x --= 10")(compile("x --= 10", MinusMinus("x".f, 10.v).doAndSet))
    it("should decompile: x --= 10")(decompile(MinusMinus("x".f, 10.v).doAndSet, "x --= 10"))
  }

  describe(classOf[NEG].getSimpleName) {
    it("should compile: -128")(compile("-128", NEG(128.v)))
    it("should decompile: -128")(decompile(NEG(128.v), "-(128)"))
    it("should evaluate: -128")(evaluate("-128", -128))
  }

  describe(classOf[Percent].getSimpleName) {
    it("should equate: z % 3")(assert(("z".f % 3.v) == Percent("z".f, 3.v)))
    it("should compile: 15 % 3")(compile("15 % 3", Percent(15.v, 3.v)))
    it("should decompile: 15 % 3")(decompile(Percent(15.v, 3.v), "15 % 3"))
    it("should evaluate: 15 % 3")(evaluate("15 % 3", 0))

    it("should equate: z %= 3")(assert(("z".f %= 3.v) == Percent("z".f, 3.v).doAndSet))
    it("should compile: x %= 3")(compile("x %= 3", Percent("x".f, 3.v).doAndSet))
    it("should decompile: x %= 7")(decompile(Percent("x".f, 7.v).doAndSet, "x %= 7"))
    it("should evaluate: x %= 3")(evaluate(
      """|var x = 50
         |x %= 3
         |x
         |""".stripMargin, 2))
  }

  describe(classOf[PercentPercent].getSimpleName) {
    it("should equate: z %% 6")(assert("z".f %% 6.v == PercentPercent("z".f, 6.v)))
    it("should compile: a %% b")(compile("a %% b", PercentPercent("a".f, "b".f)))
    it("should decompile: a %% b")(decompile(PercentPercent("a".f, "b".f), "a %% b"))

    it("should compile: a %%= b")(compile("a %%= b", PercentPercent("a".f, "b".f).doAndSet))
    it("should decompile: a %%= b")(decompile(PercentPercent("a".f, "b".f).doAndSet, "a %%= b"))
  }

  describe(classOf[Plus].getSimpleName) {
    it("should equate: z + 6") (assert("z".f + 6.v == Plus("z".f, 6.v)))
    it("should compile: 5 + 6")(compile("5 + 6", Plus(5.v, 6.v)))
    it("should decompile: 5 + 6")(decompile(Plus(5.v, 6.v), "5 + 6"))
    it("should evaluate: 5 + 6")(evaluate("5 + 6", 11))

    it("should equate: z += 6")(assert(("z".f += 6.v) == Plus("z".f, 6.v).doAndSet))
    it("should compile: x += 1")(compile("x += 1", Plus("x".f, 1.v).doAndSet))
    it("should decompile: x += 1")(decompile(Plus("x".f, 1.v).doAndSet, "x += 1"))
    it("should evaluate: x += 1")(evaluate(
      """|var x = 0
         |x += 1
         |x += 4
         |x
         |""".stripMargin, 5))
  }

  describe(classOf[PlusPlus].getSimpleName) {
    it("should equate: z ++ 6")(assert("z".f ++ 6.v == PlusPlus("z".f, 6.v)))
    it("should compile: a ++ b")(compile("a ++ b", PlusPlus("a".f, "b".f)))
    it("should decompile: a ++ b")(decompile(PlusPlus("a".f, "b".f), "a ++ b"))

    it("should compile: a ++= b")(compile("a ++= b", PlusPlus("a".f, "b".f).doAndSet))
    it("should decompile: a ++= b")(decompile(PlusPlus("a".f, "b".f).doAndSet, "a ++= b"))
  }

  describe(classOf[Times].getSimpleName) {
    it("should equate: z * 4") (assert(("z".f * 4.v) == Times("z".f, 4.v)))
    it("should compile: 7 * 4")(compile("7 * 4", Times(7.v, 4.v)))
    it("should decompile: 7 * 4")(decompile(Times(7.v, 4.v), "7 * 4"))
    it("should evaluate: 7 * 4")(evaluate("7 * 4", 28))

    it("should equate: z *= 4") (assert(("z".f *= 4.v) == Times("z".f, 4.v).doAndSet))
    it("should compile: x *= 1")(compile("x *= 1", Times("x".f, 1.v).doAndSet))
    it("should decompile: x *= 1")(decompile(Times("x".f, 1.v).doAndSet, "x *= 1"))
    it("should evaluate: x *= 3")(evaluate(
      """|var x = 5
         |x *= 3
         |x
         |""".stripMargin, 15))
  }

  describe(classOf[TimesTimes].getSimpleName) {
    it("should equate: z ** 2") (assert(("z".f ** 2.v) == TimesTimes("z".f, 2.v)))
    it("should compile: 5 ** 2")(compile("5 ** 2", TimesTimes(5.v, 2.v)))
    it("should decompile: 5 ** 2")(decompile(TimesTimes(5.v, 2.v), "5 ** 2"))
    it("should evaluate: 5 ** 2")(evaluate("5 ** 2", 25))

    it("should equate: z **= 4") (assert(("z".f **= 4.v) == TimesTimes("z".f, 4.v).doAndSet))
    it("should compile: x **= 3")(compile("x **= 3", TimesTimes("x".f, 3.v).doAndSet))
    it("should decompile: x **= 7")(decompile(TimesTimes("x".f, 7.v).doAndSet, "x **= 7"))
    it("should evaluate: x **= 3")(evaluate(
      """|var x = 2
         |x **= 3
         |x
         |""".stripMargin, 8))
  }

  describe(classOf[Up].getSimpleName) {
    it("should equate: z ^ 3")(assert(("z".f ^ 3.v) == Up("z".f, 3.v)))
    it("should compile: 9 ^ 3")(compile("9 ^ 3", Up(9.v, 3.v)))
    it("should decompile: 9 ^ 3")(decompile(Up(9.v, 3.v), "9 ^ 3"))
    it("should evaluate: 9 ^ 3")(evaluate("9 ^ 3", 10))

    it("should equate: z ^= 3")(assert(("z".f ^= 3.v) == Up("z".f, 3.v).doAndSet))
    it("should compile: x ^= 2")(compile("x ^= 2", Up("x".f, 2.v).doAndSet))
    it("should decompile: x ^= 2")(decompile(Up("x".f, 2.v).doAndSet, "x ^= 2"))
    it("should evaluate: x ^= 2")(evaluate(
      """|var x = 9
         |x ^= 2
         |x
         |""".stripMargin, 11))
  }

  def compile(sql: String, expected: Instruction): Assertion = {
    val actual = LollypopCompiler().compile(sql)
    assert(actual == expected)
  }

  def decompile(model: Instruction, expected: String): Assertion = {
    assert(model.toSQL == expected)
  }

  def evaluate(sql: String, expected: Any): Assertion = {
    val (_, _, actual) = LollypopVM.executeSQL(Scope(), sql)
    assert(actual == expected)
  }

}
