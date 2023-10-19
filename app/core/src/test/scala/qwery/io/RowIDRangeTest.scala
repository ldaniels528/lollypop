package qwery.io

import org.scalatest.funspec.AnyFunSpec

import java.nio.ByteBuffer.wrap

class RowIDRangeTest extends AnyFunSpec {

  describe(classOf[RowIDRange].getSimpleName) {

    it("should represent a range via min and max rowID") {
      val range = RowIDRange(Some(1000L), Some(1005L))
      assert(range.toList == (1000L to 1005L).toList)
    }

    it("should represent a range via a list of rowIDs") {
      val rowIDs = (1100L to 1111L).toList
      val range = RowIDRange(rowIDs)
      assert(range.toList == rowIDs)
    }

    it("should indicate if the DynamicRange is empty") {
      val range = RowIDRange()
      assert(range.isEmpty)
    }

    it("should return the first rowID via headOption") {
      val rowIDs = (1100L to 1111L).toList
      val range = RowIDRange(rowIDs)
      assert(range.headOption contains 1100L)
    }

    it("should encode/decode a list of rowIDs") {
      val rowIDs = (1000L to 1032L).toList
      val range = RowIDRange(rowIDs)
      assert(RowIDRange.decode(wrap(range.encode)).toList == rowIDs)
    }

    it("should demonstrate superior compression of rowIDs") {
      assert(RowIDRange(rowIDs = Nil).encode.length == 1)
      assert(RowIDRange(start = Some(1000L), end = None).encode.length == 9)
      assert(RowIDRange(start = None, end = Some(1000L)).encode.length == 9)
      assert(RowIDRange(start = Some(1000L), end = Some(1000L)).encode.length == 9)
      assert(RowIDRange(rowIDs = (1000L to 1000L).toList).encode.length == 9)
      assert(RowIDRange(rowIDs = (1000L to 1032L).toList).encode.length == 17)
    }

  }

}
