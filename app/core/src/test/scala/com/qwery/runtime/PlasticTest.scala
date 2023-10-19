package com.qwery.runtime

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Plastic.implicits.{MethodNameConverter, ProductToMap}
import com.qwery.runtime.Plastic.{newTypedInstance, proxyOf, seqToTuple}
import com.qwery.runtime.datatypes.BLOB
import com.qwery.util.LogUtil
import org.scalatest.funspec.AnyFunSpec

class PlasticTest extends AnyFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = ctx.createRootScope()
  implicit val cl: DynamicClassLoader = ctx.classLoader

  trait BigMartSKU extends Plastic

  val inst: BigMartSKU = newTypedInstance[BigMartSKU](
    className = "BigMartSKU",
    fieldNames = Seq("a", "b", "c"),
    fieldValues = Seq(1, 2, 3)
  )

  describe(classOf[Plastic].getSimpleName) {

    it("should decode: def $amp(a: String)") {
      assert("def $amp(a: String)".decodeName == "def &(a: String)")
    }

    it("should decode: def $bang(a: String)") {
      assert("def $bang(a: String)".decodeName == "def !(a: String)")
    }

    it("should decode: def $bar$bar(a: String)") {
      assert("def $bar$bar(a: String)".decodeName == "def ||(a: String)")
    }

    it("should decode: def $less$less$less(a: String)") {
      assert("def $less$less$less(n: Long)".decodeName == "def <<<(n: Long)")
    }

    it("should encode: def +(a: Int, b: Int)") {
      assert("def +(a: Int, b: Int)".encodeName == "def $plus(a: Int, b: Int)")
    }

    it("should encode: def &&(a: Int, b: Int)") {
      assert("def &&(a: Int, b: Int)".encodeName == "def $amp$amp(a: Int, b: Int)")
    }

    it("should convert a sequence into a tuple") {
      assert(seqToTuple((1 to 22).toList) contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22))
    }

    it("should proxy an instance") {
      var closed = false
      val inst = proxyOf[java.sql.Blob](BLOB.fromString("Hello World")) {
        case (inst, method, args) if method.getName == "free" && (args == null || args.isEmpty) =>
          closed = true
          method.invoke(inst)
      }
      inst.free()
      assert(closed)
    }

    it("should produce the appropriate toString message for a proxied Product") {
      assert(inst.toString == "BigMartSKU(1, 2, 3)")
    }

    it("should convert a native Product into a Map") {
      case class Fox(a: Int = 1, b: Int = 5, c: Int = 13)
      assert(Fox().toMap == Map("a" -> 1, "b" -> 5, "c" -> 13))
    }

    it("should convert a proxied Product into a Map") {
      assert(inst.toMap == Map("a" -> 1, "b" -> 2, "c" -> 3))
    }

    it("should retrieve the arity of a proxied Product") {
      assert(inst.productArity == 3)
    }

    it("should perform membersOf() on a proxied Product") {
      val (_, _, rc) = QweryVM.searchSQL(scope.withVariable("x", inst),
        """|membersOf(x)
           |""".stripMargin)
      rc.tabulate().foreach(LogUtil(this).info)
    }

  }

}
