package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.runtime.Scope

import java.nio.ByteBuffer.wrap

/**
 * BlobType Tests
 */
class BlobTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(BlobType.getClass.getSimpleName) {

    it("should encode/decode BlobType values") {
      val message = "Hello World".getBytes()
      val blob = BlobType.decode(wrap(BlobType.encode(message)))
      assert(blob.getBytes(0, blob.length().toInt) sameElements message)
    }

    it("should resolve 'BLOB'") {
      verifySpec(spec = "BLOB", expected = BlobType)
    }

    it("should resolve 'BLOB[5]'") {
      verifySpec(spec = "BLOB[5]", expected = ArrayType(BlobType, capacity = Some(5)))
    }

    it("should provide a SQL representation") {
      verifySQL("BLOB", BlobType)
      verifySQL("BLOB[20]", ArrayType(componentType = BlobType, capacity = Some(20)))
    }
  }

}
