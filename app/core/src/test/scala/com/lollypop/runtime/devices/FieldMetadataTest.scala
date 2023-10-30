package com.lollypop.runtime.devices

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Field Metadata Test Suite
 */
class FieldMetadataTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[FieldMetadata].getSimpleName) {

    it("should encode/decode every permutation of metadata") {
      for {
        a <- Seq(true, false)
        c <- Seq(true, false)
      } yield verify(FieldMetadata(isActive = a, isCompressed = c))
    }
  }

  private def verify(md: FieldMetadata): Assertion = {
    val code = md.encode
    logger.info(f"$md ~> [$code%02x] ${(code & 0xFF).toBinaryString.reverse.padTo(8, '0').reverse}")
    assert(FieldMetadata.decode(code) == md)
  }

}
