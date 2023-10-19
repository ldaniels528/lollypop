drop if exists Positions

create table if not exists Positions (
    position_id: UUID = UUID(),
    contest_id: UUID,
    participant_id: UUID,
    order_id: UUID,
    symbol: String(5),
    exchange: String(6),
    pricePaid: Double,
    creationTime: DateTime = DateTime()
)

create index if not exists Positions#contest_id

create index if not exists Positions#participant_id

create index if not exists Positions#position_id