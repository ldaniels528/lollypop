package com.lollypop.util

import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.io.ByteArrayOutputStream

class ResourceHelperTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(ResourceHelper.getClass.getSimpleName) {

    it("should measure the execution time of a function") {
        val (_, t) = time {
          logger.info("How slow is this?")
        }
      info(s"logger took $t msec")
      assert(t >= 0)
    }

    it("should auto-close a resource after use") {
      var isClosed = false
      val baos = new ByteArrayOutputStream(64) {
        override def close(): Unit = {
          isClosed = true
          super.close()
        }
      } use { out =>
        out.write("Hello World".getBytes)
        out
      }
      assert(baos.toByteArray sameElements "Hello World".getBytes)
      assert(isClosed)
    }

  }

}
