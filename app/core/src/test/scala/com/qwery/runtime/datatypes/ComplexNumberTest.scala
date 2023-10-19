package com.qwery.runtime.datatypes

import qwery.lang.ComplexNumber._
import org.scalatest.funspec.AnyFunSpec
import qwery.lang.ComplexNumber

/**
 * ComplexNumber Tests
 */
class ComplexNumberTest extends AnyFunSpec {

  describe(classOf[ComplexNumber].getSimpleName) {

    it("should recognize: 3i + 7") {
      assert(3.i + 7 == new ComplexNumber(imaginary = 3, constant = 7))
    }

    it("should recognize: 6i - 9") {
      assert(6.i - 9 == new ComplexNumber(imaginary = 6, constant = -9))
    }

    it("should provide a friendly toString: (6i - 9).toString") {
      assert((6.i - 9).toString == "6.0i - 9.0")
    }

    it("should verify: (3i + 7) + (6i - 9)") {
      assert((3.i + 7) + (6.i - 9) == new ComplexNumber(imaginary = 9, constant = -2))
    }

    it("should compute: (3i + 7) - (6i - 9)") {
      assert((3.i + 7) - (6.i - 9) == new ComplexNumber(imaginary = -3, constant = 16))
    }

    it("should verify: (3i + 7) * (6i - 9)") {
      assert((3.i + 7) * (6.i - 9) == new ComplexNumber(imaginary = 18, constant = -63))
    }

    it("should compute: (3i + 7) / (6i - 9)") {
      assert((3.i + 7) / (6.i - 9) == new ComplexNumber(imaginary = 0.5, constant = -0.7777777777777778))
    }

    it("should compute: (3i + 7) % (6i - 9)") {
      assert((3.i + 7) % (6.i - 9) == new ComplexNumber(imaginary = 3, constant = 7.0))
    }

    it("should compute: (3i + 7) * 6") {
      assert((3.i + 7) * 6 == new ComplexNumber(imaginary = 18, constant = 42))
    }

    it("should compute: (35i + 7) / 7") {
      assert((35.i + 7) / 7 == new ComplexNumber(imaginary = 5, constant = 1))
    }

    it("should compute: sqrt(-100)") {
      assert(ComplexNumber.sqrt(-100) == new ComplexNumber(imaginary = 10, constant = 0))
    }

    it("should compute: sqrt(100)") {
      assert(ComplexNumber.sqrt(100) == new ComplexNumber(imaginary = 0, constant = 10))
    }

  }

}
