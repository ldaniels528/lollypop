package com.lollypop.runtime.devices

import com.lollypop.language.models.{Condition, Expression}
import com.lollypop.runtime.{ROWID, Scope}
import lollypop.io.IOCost
import org.slf4j.LoggerFactory

import java.text.NumberFormat
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

/**
 * Statistics Generating Row Collection
 * @param host the host [[RowCollection Row Collection]]
 */
case class StatsGeneratingRowCollection(host: RowCollection) extends HostedRowCollection {
  private val logger = LoggerFactory.getLogger(getClass)
  private val successes: AtomicInteger = new AtomicInteger()
  private val failures: AtomicInteger = new AtomicInteger()
  private val bytes: AtomicInteger = new AtomicInteger()
  private val nf = NumberFormat.getNumberInstance

  val (_SECONDS, _KILOBYTES) = (1e+3, 1024.0)
  var lastBytes: Int = 0
  var lastRows: Int = 0
  val startTime: Long = System.currentTimeMillis()
  var lastUpdate: Long = startTime

  override def insert(record: Row): IOCost = {
    val cost = super.insert(record)
    logUpdate(cost)
    cost
  }

  override def insert(rows: RecordCollection[Row]): IOCost = {
    val cost = super.insert(rows)
    logUpdate(cost)
    cost
  }

  override def insertInside(innerTableColumn: TableColumn, injectColumns: Seq[String], injectValues: Seq[Seq[Expression]], condition: Option[Condition], limit: Option[Expression])(implicit scope: Scope): IOCost = {
    val cost = super.insertInside(innerTableColumn, injectColumns, injectValues, condition, limit)
    logUpdate(cost)
    cost
  }

  override def swap(rowID0: ROWID, rowID1: ROWID): IOCost = {
    val cost = super.swap(rowID0, rowID1)
    logUpdate(cost)
    cost
  }

  override def update(rowID: ROWID, row: Row): IOCost = {
    val cost = super.update(rowID, row)
    logUpdate(cost)
    cost
  }

  override def upsert(row: Row, condition: Condition)(implicit scope: Scope): IOCost = {
    val cost = super.upsert(row, condition)
    logUpdate(cost)
    cost
  }

  private def logUpdate(cost: IOCost): Unit = {
    successes.addAndGet(cost.updated + cost.inserted)
    bytes.addAndGet(recordSize)
    val currentTime = System.currentTimeMillis()
    if (currentTime - lastUpdate >= 1.seconds.toMillis) {
      val (totalBytes, totalRows) = (bytes.get(), successes.get() + failures.get())
      val cycleTime = (currentTime - lastUpdate) / _SECONDS
      val processTime = (currentTime - startTime) / _SECONDS
      val kbps = (totalBytes - lastBytes) / _KILOBYTES / cycleTime
      val avgKbps = totalBytes / _KILOBYTES / processTime
      val rps = (totalRows - lastRows) / cycleTime
      val avgRps = totalRows / processTime
      logger.info(
        (s"${nf.format(successes.get())} rows" ::
          f"$rps%.1f row/s" ::
          f"$avgRps%.1f row/s (avg)" ::
          (if (failures.get() > 0) List(s"${nf.format(failures.get())} errors") else Nil) :::
          toKB(totalBytes) ::
          f"$kbps%.1f KB/s" ::
          f"$avgKbps%.1f KB/s (avg)" :: Nil).mkString(" | "))
      lastBytes = totalBytes
      lastUpdate = currentTime
      lastRows = totalRows
    }
  }

  private def toKB(number: Double): String = {
    val units = Seq("B", "KB", "MB", "GB", "TB", "PB")
    var unit: Int = 0
    var n: Double = number
    while (n > _KILOBYTES && unit < units.size) {
      n /= _KILOBYTES
      unit += 1
    }
    f"$n%.1f ${units(unit)}"
  }

}
