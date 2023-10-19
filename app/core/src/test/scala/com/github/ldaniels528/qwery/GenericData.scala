package com.github.ldaniels528.qwery

import com.qwery.runtime.ColumnInfo

import scala.annotation.meta.field

case class GenericData(@(ColumnInfo@field)(typeDef = "RowNumber") _id: Long = 0L,
                       @(ColumnInfo@field)(maxSize = 5) idValue: String,
                       @(ColumnInfo@field)(maxSize = 5) idType: String,
                       responseTime: Int,
                       reportDate: Long)
