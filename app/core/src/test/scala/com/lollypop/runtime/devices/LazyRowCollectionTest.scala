package com.lollypop.runtime.devices

import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import org.scalatest.funspec.AnyFunSpec
import lollypop.lang.OS.{createFileTable, toFileRow}

import java.io.File

class LazyRowCollectionTest extends AnyFunSpec {

  describe(classOf[LazyRowCollection].getSimpleName) {
    val filterFiles: File => Boolean = _.getName.endsWith(".sql")
    val filterKeys: ((String, Any)) => Boolean = t => Set("canonicalPath", "lastModified", "length").contains(t._1)

    it("should lazily read rows from an iterator") {
      val files = new File("./contrib/examples/src/main/lollypop").streamFiles
      implicit val out: RowCollection = createFileTable()
      val rows = files.filter(filterFiles).sortBy(_.getName).map(toFileRow).iterator
      val lazyRC = LazyRowCollection(out, rows)
      assert(lazyRC(0).toMap.filterNot(filterKeys) == Map(
        "name" -> "BlackJack.sql", "isHidden" -> false, "isDirectory" -> false, "isFile" -> true
      ))
      assert(lazyRC(1).toMap.filterNot(filterKeys) == Map(
        "name" -> "BreakOutDemo.sql", "isHidden" -> false, "isDirectory" -> false, "isFile" -> true
      ))
      assert(rows.hasNext)
    }

    it("should use getLength() to determine the length of the device") {
      val files = new File("./contrib/examples/src/main/lollypop").streamFiles
      implicit val out: RowCollection = createFileTable()
      val rows = files.filter(filterFiles).map(toFileRow).iterator
      val lazyRC = LazyRowCollection(out, rows)
      assert(lazyRC.getLength == 7)
      assert(!rows.hasNext)
    }

  }

}
