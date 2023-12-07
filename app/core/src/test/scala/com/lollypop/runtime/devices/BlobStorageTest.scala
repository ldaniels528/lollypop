package com.lollypop.runtime.devices

import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.{CLOB, StringType}
import lollypop.lang.Pointer
import org.scalatest.funspec.AnyFunSpec

class BlobStorageTest extends AnyFunSpec {

  describe(classOf[BlobStorage].getSimpleName) {
    val ref = DatabaseObjectRef("BlobStorageTest")

    it("should update (overwrite) columns in BLOB storage") {
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(), createPassengerDataSQL(ref))
      assert((cost0.created == 1) & (cost0.inserted == 4))
      LogicalTableRowCollection(ref)(scope0) use { table =>
        assert(table.clustered.toMapGraph == List(
          Map("airportCode" -> "DTW", "age" -> 35, "lastName" -> Pointer(0, 10, 10), "firstName" -> Pointer(10, 7, 7), "id" -> 0),
          Map("airportCode" -> "SNA", "age" -> 32, "lastName" -> Pointer(17, 10, 10), "firstName" -> Pointer(27, 8, 8), "id" -> 1),
          Map("airportCode" -> "DFW", "age" -> 68, "lastName" -> Pointer(35, 9, 9), "firstName" -> Pointer(44, 8, 8), "id" -> 2),
          Map("airportCode" -> "LAX", "age" -> 71, "lastName" -> Pointer(52, 11, 11), "firstName" -> Pointer(63, 8, 8), "id" -> 3)
        ))

        // overwrite a column
        val blobStorage = table.blob
        val cost1 = blobStorage.write(rowID = 3, columnID = 2, StringType.copy(isExternal = true).encode("Fred"))
        assert(cost1.updated == 1)
        assert(table.clustered.toMapGraph == List(
          Map("airportCode" -> "DTW", "age" -> 35, "lastName" -> Pointer(0, 10, 10), "firstName" -> Pointer(10, 7, 7), "id" -> 0),
          Map("airportCode" -> "SNA", "age" -> 32, "lastName" -> Pointer(17, 10, 10), "firstName" -> Pointer(27, 8, 8), "id" -> 1),
          Map("airportCode" -> "DFW", "age" -> 68, "lastName" -> Pointer(35, 9, 9), "firstName" -> Pointer(44, 8, 8), "id" -> 2),
          Map("airportCode" -> "LAX", "age" -> 71, "lastName" -> Pointer(52, 11, 11), "firstName" -> Pointer(63, 8, 8), "id" -> 3)
        ))
        assert(table.toMapGraph == List(
          Map("id" -> 0, "airportCode" -> "DTW", "age" -> 35, "lastName" -> CLOB("Thomas"), "firstName" -> CLOB("Dan")),
          Map("id" -> 1, "airportCode" -> "SNA", "age" -> 32, "lastName" -> CLOB("Thomas"), "firstName" -> CLOB("Mary")),
          Map("id" -> 2, "airportCode" -> "DFW", "age" -> 68, "lastName" -> CLOB("Hamil"), "firstName" -> CLOB("John")),
          Map("id" -> 3, "airportCode" -> "LAX", "age" -> 71, "lastName" -> CLOB("Russell"), "firstName" -> CLOB("Fred"))
        ))
      }
    }

    it("should update (replace) columns in BLOB storage") {
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(), createPassengerDataSQL(ref))
      assert((cost0.created == 1) & (cost0.inserted == 4))
      LogicalTableRowCollection(ref)(scope0) use { table =>
        assert(table.clustered.toMapGraph == List(
          Map("airportCode" -> "DTW", "age" -> 35, "lastName" -> Pointer(0, 10, 10), "firstName" -> Pointer(10, 7, 7), "id" -> 0),
          Map("airportCode" -> "SNA", "age" -> 32, "lastName" -> Pointer(17, 10, 10), "firstName" -> Pointer(27, 8, 8), "id" -> 1),
          Map("airportCode" -> "DFW", "age" -> 68, "lastName" -> Pointer(35, 9, 9), "firstName" -> Pointer(44, 8, 8), "id" -> 2),
          Map("airportCode" -> "LAX", "age" -> 71, "lastName" -> Pointer(52, 11, 11), "firstName" -> Pointer(63, 8, 8), "id" -> 3)
        ))

        // overwrite a column
        val blobStorage = table.blob
        val cost1 = blobStorage.write(rowID = 3, columnID = 2, StringType.copy(isExternal = true).encode("Caroline"))
        assert(cost1.updated == 1)
        assert(table.clustered.toMapGraph == List(
          Map("airportCode" -> "DTW", "age" -> 35, "lastName" -> Pointer(0, 10, 10), "firstName" -> Pointer(10, 7, 7), "id" -> 0),
          Map("airportCode" -> "SNA", "age" -> 32, "lastName" -> Pointer(17, 10, 10), "firstName" -> Pointer(27, 8, 8), "id" -> 1),
          Map("airportCode" -> "DFW", "age" -> 68, "lastName" -> Pointer(35, 9, 9), "firstName" -> Pointer(44, 8, 8), "id" -> 2),
          Map("airportCode" -> "LAX", "age" -> 71, "lastName" -> Pointer(52, 11, 11), "firstName" -> Pointer(71, 12, 12), "id" -> 3)
        ))
        assert(table.toMapGraph == List(
          Map("id" -> 0, "airportCode" -> "DTW", "age" -> 35, "lastName" -> CLOB("Thomas"), "firstName" -> CLOB("Dan")),
          Map("id" -> 1, "airportCode" -> "SNA", "age" -> 32, "lastName" -> CLOB("Thomas"), "firstName" -> CLOB("Mary")),
          Map("id" -> 2, "airportCode" -> "DFW", "age" -> 68, "lastName" -> CLOB("Hamil"), "firstName" -> CLOB("John")),
          Map("id" -> 3, "airportCode" -> "LAX", "age" -> 71, "lastName" -> CLOB("Russell"), "firstName" -> CLOB("Caroline"))
        ))
      }
    }

    it("should delete columns from BLOB storage") {
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(), createPassengerDataSQL(ref))
      assert((cost0.created == 1) & (cost0.inserted == 4))
      LogicalTableRowCollection(ref)(scope0) use { table =>
        // delete a column
        val blobStorage = table.blob
        val cost1 = blobStorage.delete(rowID = 3, columnID = 2)
        assert(cost1.deleted == 1)
        assert(table.clustered.toMapGraph == List(
          Map("airportCode" -> "DTW", "age" -> 35, "lastName" -> Pointer(0, 10, 10), "firstName" -> Pointer(10, 7, 7), "id" -> 0),
          Map("airportCode" -> "SNA", "age" -> 32, "lastName" -> Pointer(17, 10, 10), "firstName" -> Pointer(27, 8, 8), "id" -> 1),
          Map("airportCode" -> "DFW", "age" -> 68, "lastName" -> Pointer(35, 9, 9), "firstName" -> Pointer(44, 8, 8), "id" -> 2),
          Map("id" -> 3, "lastName" -> Pointer(52, 11, 11), "age" -> 71, "airportCode" -> "LAX")
        ))
        assert(table.toMapGraph == List(
          Map("id" -> 0, "airportCode" -> "DTW", "age" -> 35, "lastName" -> CLOB("Thomas"), "firstName" -> CLOB("Dan")),
          Map("id" -> 1, "airportCode" -> "SNA", "age" -> 32, "lastName" -> CLOB("Thomas"), "firstName" -> CLOB("Mary")),
          Map("id" -> 2, "airportCode" -> "DFW", "age" -> 68, "lastName" -> CLOB("Hamil"), "firstName" -> CLOB("John")),
          Map("id" -> 3, "airportCode" -> "LAX", "age" -> 71, "lastName" -> CLOB("Russell"))
        ))
        assert(blobStorage.journal.entries.toMapGraph == List(
          Map("rowID" -> 3, "columID" -> 2, "ptr" -> Pointer(63, 8, 8))
        ))
      }
    }

  }

  private def createPassengerDataSQL(ref: DatabaseObjectRef): String = {
    s"""|drop if exists $ref &&
        |create table $ref (
        |   id: RowNumber,
        |   lastName: CLOB,
        |   firstName: CLOB,
        |   age: Int,
        |   airportCode: String(3) = 'LAX'
        |) &&
        |insert into $ref (firstName, lastName, age, airportCode)
        |values ('Dan', 'Thomas', 35, 'DTW'),
        |       ('Mary', 'Thomas', 32, 'SNA'),
        |       ('John', 'Hamil', 68, 'DFW'),
        |       ('Kurt', 'Russell', 71, 'LAX')
        |""".stripMargin
  }

}
