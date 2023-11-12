drop if exists Orders

create table if not exists Orders (
    order_id: UUID = UUID(),
    contest_id: UUID,
    participant_id: UUID,
    order_type: Enum (BUY, SELL),
    order_terms: Enum (`LIMIT`, MARKET),
    symbol: String(5),
    exchange: String(6),
    price: Double,
    creationTime: DateTime = DateTime(),
    expirationTime: DateTime = DateTime() + Duration('3 days')
)

create index if not exists Orders#contest_id

create index if not exists Orders#participant_id

create index if not exists Orders#order_id