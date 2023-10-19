package com.qwery.util

import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class ClassPathHelperTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[ClassPathHelper.type].getSimpleName) {

    it("should return classes from the class path") {
      val classes = ClassPathHelper.searchClassPath(pattern = "scala[.]collection[.]Array(.*)")
      logger.info(s"${classes.length} classes")
      classes.foreach(logger.info)
    }

  }

}
