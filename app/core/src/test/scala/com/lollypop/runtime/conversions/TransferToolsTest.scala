package com.lollypop.runtime.conversions

import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import org.apache.commons.io.output.ByteArrayOutputStream
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class TransferToolsTest extends AnyFunSpec {

  describe(classOf[TransferTools].getSimpleName) {

    it("should copy a file to a stream") {
      val srcFile = new File("app") / "core" / "src" / "main" / "resources" / "log4j.properties"
      val out = new ByteArrayOutputStream(srcFile.length().toInt)
      val count = TransferTools.transfer(srcFile, out)
      assert(count == 330)
    }

  }

}
