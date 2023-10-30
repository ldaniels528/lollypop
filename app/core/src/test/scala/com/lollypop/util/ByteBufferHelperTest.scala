package com.lollypop.util

import com.lollypop.runtime.LONG_BYTES
import com.lollypop.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}
import org.scalatest.funspec.AnyFunSpec

import java.nio.ByteBuffer
import java.util.{Date, UUID}
import scala.concurrent.duration.DurationInt

object ByteBufferHelperTest extends AnyFunSpec {

  describe(classOf[ByteBufferHelper.type].getSimpleName) {

    it("should encode/decode a Date") {
      val dateA = new Date()
      val buf = ByteBuffer.allocate(LONG_BYTES).putDate(dateA).flipMe()
      val dateB = buf.getDate
      assert(dateA == dateB)
    }

    it("should encode/decode an Interval (FiniteDuration)") {
      val intervalA = 1234.millis
      val buf = ByteBuffer.allocate(LONG_BYTES * 2).putInterval(intervalA).flipMe()
      val intervalB = buf.getInterval
      assert(intervalA == intervalB)
    }

    it("should encode/decode a Row ID") {
      val rowIdA = 0xDEADBEEFL
      val buf = ByteBuffer.allocate(LONG_BYTES).putRowID(rowIdA).flipMe()
      val rowIdB = buf.getRowID
      assert(rowIdA == rowIdB)
    }

    it("should encode/decode a UUID") {
      val uuidA = UUID.randomUUID()
      val buf = ByteBuffer.allocate(LONG_BYTES * 2).putUUID(uuidA).flipMe()
      val uuidB = buf.getUUID
      assert(uuidA == uuidB)
    }

  }

}
