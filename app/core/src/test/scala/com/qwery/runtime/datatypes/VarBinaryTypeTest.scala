package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope

import java.nio.ByteBuffer

class VarBinaryTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = ctx.createRootScope()

  describe(classOf[VarBinaryType].getSimpleName) {
    
    it("should encode/decode VarBinary values") {
      val message = "Hello World".getBytes()
      assert(VarBinaryType.decode(ByteBuffer.wrap(VarBinaryType.encode(message))) sameElements message)
    }

    it("should resolve 'VarBinary'") {
      verifySpec(spec = "VarBinary", expected = VarBinaryType)
    }

    it("should resolve 'VarBinary[5]'") {
      verifySpec(spec = "VarBinary[5]", expected = ArrayType(VarBinaryType, capacity = Some(5)))
    }

    it("should provide a SQL representation: VarBinary(100)") {
      verifySQL("VarBinary(100)", VarBinaryType(100))
    }

    it("should provide a SQL representation: VarBinary(500)[20]") {
      verifySQL("VarBinary(500)[20]", ArrayType(componentType = VarBinaryType(500), capacity = Some(20)))
    }
    
  }

}
