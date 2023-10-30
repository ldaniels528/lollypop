package com.lollypop.runtime.devices

import com.lollypop.runtime.ROWID

case class RowSummary(active: ROWID = 0, compressed: ROWID = 0, deleted: ROWID = 0, encrypted: ROWID = 0, locked: ROWID = 0, replicated: ROWID = 0) {
  def +(that: RowSummary): RowSummary = that.copy(
    active = that.active + active,
    compressed = that.compressed + compressed,
    deleted = that.deleted + deleted,
    encrypted = that.encrypted + encrypted,
    locked = that.locked + locked,
    replicated = that.replicated + replicated
  )
}
