drop if exists Members

create table if not exists Members (
    member_id: UUID = UUID(),
    name: String(32),
    funds: Double = 100000,
    creationTime: DateTime = DateTime()
)

create index if not exists Members#member_id