package com.lollypop.runtime

import com.lollypop.runtime.datatypes.BLOB
import com.lollypop.util.StringRenderHelper.StringRenderer
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class ResourceManagerTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[ResourceManager.type].getSimpleName) {

    it("should track data object resources") {
      val blob = BLOB.fromString("Hello World")
      assert(ResourceManager.getResource(blob.ns) contains blob)
      blob.free()
    }

    it("should close and remove a resource by namespace") {
      val blob = BLOB.fromString("Hello World")
      ResourceManager.close(blob.ns)
      assert(ResourceManager.getResource(blob.ns).isEmpty)
    }

    it("should return a table containing all tracked resources") {
      val rc = ResourceManager.getResources
      rc.tabulate().foreach(logger.info)
    }

    it("should return a mapping containing all tracked resources") {
      val mapping = ResourceManager.getResourceMappings
      mapping.foreach { case (name, value) =>
        logger.info(s"$name: ${value.render}")
      }
    }

  }

}
