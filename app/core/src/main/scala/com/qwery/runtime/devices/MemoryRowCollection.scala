package com.qwery.runtime.devices

/**
 * Represents an in-memory row-based database collection
 */
trait MemoryRowCollection extends RowCollection {
  override def isMemoryResident: Boolean = true
}
