package com.qwery.runtime.instructions.queryables

import com.qwery.language.models.Condition
import com.qwery.runtime.devices.{CursorSupport, RowCollection}

case class JoinSource(tableAlias: String,
                      device: RowCollection with CursorSupport,
                      joinColumnNames: List[String],
                      condition: Option[Condition])
