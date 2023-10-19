package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope

import java.nio.ByteBuffer

class VarCharTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = ctx.createRootScope()

  describe(classOf[VarCharType].getSimpleName) {

    it("should encode/decode VarChar values") {
      val message = "Hello World".toCharArray
      assert(VarCharType.decode(ByteBuffer.wrap(VarCharType.encode(message))) sameElements message)
    }

    it("should resolve 'VarChar'") {
      verifySpec(spec = "VarChar", expected = VarCharType)
    }

    it("should resolve 'VarChar[5]'") {
      verifySpec(spec = "VarChar[5]", expected = ArrayType(VarCharType, capacity = Some(5)))
    }

    it("should provide a SQL representation: VarChar(100)") {
      verifySQL("VarChar(100)", VarCharType(100))
    }

    it("should provide a SQL representation: VarChar(500)[20]") {
      verifySQL("VarChar(500)[20]", ArrayType(componentType = VarCharType(500), capacity = Some(20)))
    }

  }

}
