package com.qwery.database.jdbc.types

import java.sql.Wrapper

trait JDBCWrapper extends Wrapper {

  override final def unwrap[T](iface: Class[T]): T = ??? // TODO implement me

  override final def isWrapperFor(iface: Class[_]): Boolean = false

}
