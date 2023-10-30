package com.lollypop.runtime.datatypes

import com.lollypop.runtime.Scope

class BitArrayTypeTest extends DataTypeFunSpec {
  implicit val scope: Scope = Scope()

  describe(classOf[BitArrayType].getSimpleName) {

    it("should resolve 'BitArray(256)'") {
      verifySpec(spec = "BitArray(256)", expected = BitArrayType(256))
    }

    it("should resolve 'BitArray(256)[5]'") {
      verifySpec(spec = "BitArray(256)[5]", expected = ArrayType(BitArrayType(256), capacity = Some(5)))
    }

    it("should resolve 'BitArray(1024)*'") {
      verifySpec(spec = "BitArray(1024)*", expected = BitArrayType(1024, isExternal = true))
    }

    it("should provide a SQL representation") {
      verifySQL("BitArray(32)", BitArrayType(32))
      verifySQL("BitArray(32)[20]", ArrayType(componentType = BitArrayType(32), capacity = Some(20)))
    }

  }

}
