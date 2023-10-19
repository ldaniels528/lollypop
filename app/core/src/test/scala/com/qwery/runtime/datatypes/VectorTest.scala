package com.qwery.runtime.datatypes

import com.qwery.runtime.datatypes.Vector.{RichMatrixVector, Vector}
import org.scalatest.funspec.AnyFunSpec

class VectorTest extends AnyFunSpec {

  describe(classOf[Vector.type].getSimpleName) {

    it("should compute a dot product") {
      val vectorA: Vector = Vector(1.0, 2.0, 3.0)
      val vectorB: Vector = Vector(2.0, 5.0, 6.0)
      val result = vectorA.dotProduct(vectorB)
      assert(result == 30.0)
    }

    it("should compute a normalized vector") {
      val vectorB: Vector = Vector(2.0, 5.0, 6.0)
      val result = vectorB.normalizeVector
      assert(result sameElements Vector(0.24806946917841693, 0.6201736729460423, 0.7442084075352507))
    }

  }

}
