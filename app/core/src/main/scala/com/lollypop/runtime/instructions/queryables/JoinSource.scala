package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.Condition
import com.lollypop.runtime.devices.{CursorSupport, RowCollection}

case class JoinSource(tableAlias: String,
                      device: RowCollection with CursorSupport,
                      joinColumnNames: List[String],
                      condition: Option[Condition])
