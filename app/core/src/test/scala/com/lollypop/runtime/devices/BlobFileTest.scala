package com.lollypop.runtime.devices

import com.lollypop.runtime._
import com.lollypop.runtime.devices.RowCollectionZoo.{createTempFile, createTempNS}
import lollypop.lang.Pointer
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.io.File
import scala.io.Source

class BlobFileTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[BlobFile].getSimpleName) {

    it("should read and write data of any length") {
      BlobFile(createTempNS(), createTempFile()) use { implicit device =>
        // write "Hello World!!!" the device
        val w0: Pointer = write(label = "a0", message = "Hello World!!!")
        assert(w0.offset == 0 && w0.length == 14)

        // read "Hello World!!!" from on the device
        val r0 = read(label = "a0", ref = w0)
        assert(r0 sameElements "Hello World!!!".getBytes)

        // write "Goodbye World!!!" the device
        val w1: Pointer = write(label = "b0", message = "Goodbye World!!!")
        assert(w1.offset == 14 && w1.length == 16)

        // update "Goodbye World!!!" with "Goodbye Cruel World!!!" at position w1 on the device
        val w2: Pointer = overwrite(label = "b1", message = "Goodbye Cruel World!!!", ref = w1)
        assert(w2.offset == 14 && w2.length == 22)

        // read "Goodbye Cruel World!!!" from the device
        val r1 = read(label = "b1", ref = w2)
        assert(r1 sameElements "Goodbye Cruel World!!!".getBytes)

        // update "Goodbye World!!!" with "What's up?" at position w1 on the device
        val w3: Pointer = overwrite(label = "b2", message = "What's up?", ref = w1)
        assert(w3.offset == 14 && w3.length == 10)

        // read "What's up?" from the device
        val r2 = read(label = "b2", ref = w3)
        assert(r2 sameElements "What's up?".getBytes)
      }
    }

    it("should upload/download files") {
      BlobFile(createTempNS(), createTempFile()) use { implicit device =>
        val inputFile = new File("./build.sbt")
        logger.info(s"file '${inputFile.getName}' is ${inputFile.length()} bytes")

        val ptr: Pointer = device.upload(inputFile)
        logger.info(s"uploaded '${inputFile.getName}' to $ptr")

        val bytes = device.read(ptr)
        logger.info(s"read ${bytes.length} bytes")
        Source.fromBytes(bytes).getLines().zipWithIndex.take(20) foreach { case (s, n) => logger.info(f"[${n + 1}%02d] $s") }
      }
    }

  }

  def read(label: String, ref: Pointer)(implicit device: BlobFile): Array[Byte] = {
    val bytes = device.read(ref)
    logger.info(s"$label: read '${new String(bytes)}' from #${ref.offset} ~> $ref")
    bytes
  }

  def overwrite(label: String, message: String, ref: Pointer)(implicit device: BlobFile): Pointer = {
    val bytes = message.getBytes()
    val w: Pointer = device.update(ref, bytes)
    if (ref.offset == w.offset)
      logger.info(s"$label: overwrote #${ref.offset} '$message' (${w.length} bytes) to #${w.offset} ~> $ref")
    else
      logger.info(s"$label: updated (new) #${ref.offset} '$message' (${w.length} bytes) to #${w.offset} ~> $ref")
    w
  }

  def write(label: String, message: String)(implicit device: BlobFile): Pointer = {
    val bytes = message.getBytes()
    val w: Pointer = device.append(bytes)
    logger.info(s"$label: wrote '$message' (${w.length} bytes) to #${w.offset} ~> $w")
    w
  }

}
