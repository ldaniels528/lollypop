//////////////////////////////////////////////////////////////////////////////////////
// Common API functions
//////////////////////////////////////////////////////////////////////////////////////

val isDateTime = (value: Any) => value.isDateTime()

val isNotNull = (value: Any) => value isnt null

val isUUID = (value: Any) => value.isUUID()

def url_encode(value: String) := {
    URLEncoder.encode(value, 'utf-8')
}

def inserted_id(tableName: String, result: Any) := {
    val rowID = result.rowIDs().toArray()[0]
    val myTable = ns(tableName)
    myTable.show()
    myTable[rowID][0]
}
