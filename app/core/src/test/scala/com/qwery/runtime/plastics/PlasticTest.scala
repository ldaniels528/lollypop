package com.qwery.runtime.plastics

import com.qwery.language.QweryUniverse
import com.qwery.runtime.datatypes.BLOB
import com.qwery.runtime.plastics.Plastic.implicits.MethodNameConverter
import com.qwery.runtime.plastics.Plastic.wrap
import com.qwery.runtime.{DynamicClassLoader, Scope}
import org.scalatest.funspec.AnyFunSpec

class PlasticTest extends AnyFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = ctx.createRootScope()
  implicit val cl: DynamicClassLoader = ctx.classLoader

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

    it("should encode: def ||(a, b)") {
      assert("def ||(a, b)".encodeName == "def $bar$bar(a, b)")
    }

    it("should wrap an instance in Plastic") {
      var closed = false
      val inst = wrap[java.sql.Blob](BLOB.fromString("Hello World")) {
        case (inst, method, args) if method.getName == "free" && (args == null || args.isEmpty) =>
          closed = true
          method.invoke(inst)
      }
      inst.free()
      assert(closed)
    }

  }

}
