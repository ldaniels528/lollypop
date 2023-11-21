package com.lollypop.runtime.devices

import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseManagementSystem, DatabaseObjectRef, Scope}
import com.lollypop.util.ResourceHelper._
import com.lollypop.runtime.conversions.TransferTools.RichInputStream
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import scala.io.Source

class InlineBlobRowCollectionTest extends AnyFunSpec with VerificationTools {
  private val ref = DatabaseObjectRef(getTestTableName)
  private val file = new File("app") / "examples" / "src" / "main" / "lollypop" / "IngestDemo.sql"

  describe(classOf[InlineBlobRowCollection].getSimpleName) {

    it("should perform CRUD operations on BLOB-encoded rows") {
      implicit val rootScope: Scope = Scope()
      val tableType = TableType(Seq(
        TableColumn(name = "filename", `type` = StringType(32)),
        TableColumn(name = "contents", `type` = BlobType)
      ))

      val ns = ref.toNS
      DatabaseManagementSystem.dropObject(ns, ifExists = true)
      DatabaseManagementSystem.createPhysicalTable(ns, tableType, ifNotExists = false)
      val device = InlineBlobRowCollection(ns)
      device.insert(Map[String, Any]("filename" -> file.getName, "contents" -> BLOB.fromFile(file)).toRow(device))
      device.close()
    }

    it("should retrieve a BLOB-encoded field") {
      implicit val rootScope: Scope = Scope()
      val contents = InlineBlobRowCollection(ref.toNS).use { device =>
        val blob = device.readField(rowID = 0, columnID = 1).value.collect { case b: IBLOB => b }.orNull
        blob.getBinaryStream.mkString()
      }
      assert(contents.trim == Source.fromFile(file).use(_.mkString).trim)
    }

  }

}
