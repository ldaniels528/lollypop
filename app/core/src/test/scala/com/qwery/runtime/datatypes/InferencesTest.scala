package com.qwery.runtime.datatypes

import com.qwery.runtime.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.qwery.runtime.datatypes.Inferences.resolveType
import com.qwery.runtime.devices.{ExternalFilesTableRowCollection, RowCollection}
import org.scalatest.funspec.AnyFunSpec

/**
 * Inferences Tests
 */
class InferencesTest extends AnyFunSpec {

  describe(classOf[Inferences].getSimpleName) {

    it("should detect subclasses") {
      assert(classOf[ExternalFilesTableRowCollection] isDescendantOf classOf[RowCollection])
    }

    it("should choose Double over all smaller numeric types") {
      assert(resolveType(Float64Type, Float32Type, Int64Type, Int32Type, Int16Type, Int8Type) == Float64Type)
    }

    it("should choose Long over all smaller numeric types") {
      assert(resolveType(Int64Type, Int32Type, Int16Type, Int8Type) == Int64Type)
    }

    it("should choose Int32Type over all smaller numeric types") {
      assert(resolveType(Int32Type, Int16Type, Int8Type) == Int32Type)
    }

    it("should choose Int16Type over all smaller numeric types") {
      assert(resolveType(Int16Type, Int8Type) == Int16Type)
    }

    it("should choose String over other compatible types") {
      assert(resolveType(StringType, CharType, ClobType, Float64Type, Float32Type, Int64Type, Int32Type, Int16Type, Int8Type) == StringType)
    }

  }

}
