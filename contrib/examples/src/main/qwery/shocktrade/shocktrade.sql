//////////////////////////////////////////////////////////////////////////////////////
// Title: ShockTrade Demo
// include('./contrib/examples/src/main/qwery/shocktrade/shocktrade.sql')
//////////////////////////////////////////////////////////////////////////////////////
namespace 'examples.shocktrade'

require ['org.slf4j:slf4j-api:2.0.5']

import [
    'java.net.URLEncoder',
    'org.slf4j.LoggerFactory'
]

// get the logger
val logger = LoggerFactory.getLogger('ShockTradeDemo')
logger.info('Starting ShockTrade Demo Service...')

//////////////////////////////////////////////////////////////////////////////////////
// create the tables
//////////////////////////////////////////////////////////////////////////////////////

// StockQuotes table
include('./contrib/examples/src/main/qwery/shocktrade/tables/StockQuotes.sql')

// Members table
include('./contrib/examples/src/main/qwery/shocktrade/tables/Members.sql')

// Contests table
include('./contrib/examples/src/main/qwery/shocktrade/tables/Contests.sql')

// Participants table
include('./contrib/examples/src/main/qwery/shocktrade/tables/Participants.sql')

// Orders table
include('./contrib/examples/src/main/qwery/shocktrade/tables/Orders.sql')

// Positions table
include('./contrib/examples/src/main/qwery/shocktrade/tables/Positions.sql')

//////////////////////////////////////////////////////////////////////////////////////
// startup a server
//////////////////////////////////////////////////////////////////////////////////////

val host = '0.0.0.0'
val port = nodeStart(8080)
logger.info('Started ShockTrade Demo Service on {{host}}:{{port}}...')

// setup the web socket handler
nodeAPI(port, '/ws/shocktrade', {
  ws: (message: String) => {
    val js = message.fromJson()
    return upsert into StockQuotes (symbol, exchange, lastSale, lastSaleTime)
           values (js.symbol, js.exchange, js.lastSale, js.lastSaleTime)
           where symbol is js.symbol
  }
})

// Common Utilities
include('./contrib/examples/src/main/qwery/shocktrade/utils/commons.sql')

// Stock APIs
include('./contrib/examples/src/main/qwery/shocktrade/api/stock_api.sql')

// Member APIs
include('./contrib/examples/src/main/qwery/shocktrade/api/member_api.sql')

// Contest APIs
include('./contrib/examples/src/main/qwery/shocktrade/api/contest_api.sql')

// Contest Participants APIs
include('./contrib/examples/src/main/qwery/shocktrade/api/participant_api.sql')

// Contest Orders APIs
include('./contrib/examples/src/main/qwery/shocktrade/api/order_api.sql')

// Contest Positions APIs
include('./contrib/examples/src/main/qwery/shocktrade/api/position_api.sql')

