package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

import java.util.UUID

/**
 * UUIDType Tests
 */
class UUIDTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(UUIDType.getClass.getSimpleName) {

    it("should detect UUIDType column types") {
      verifyType(value = UUID.randomUUID(), expectedType = UUIDType)
    }

    it("should encode/decode UUIDType values") {
      verifyCodec(UUIDType, value = UUID.randomUUID())
    }

    it("should conversions to UUIDType") {
      assert("d0e8f468-6de8-4baa-9210-89fc634c24b1".convertTo(UUIDType) == UUID.fromString("d0e8f468-6de8-4baa-9210-89fc634c24b1"))
    }

    it("should resolve 'UUID'") {
      verifySpec(spec = "UUID", expected = UUIDType)
    }

    it("should resolve 'UUID[4]'") {
      verifySpec(spec = "UUID[4]", expected = ArrayType(UUIDType, capacity = Some(4)))
    }

    it("should provide a SQL representation") {
      verifySQL("UUID", UUIDType)
    }

  }

}
