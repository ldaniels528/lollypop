drop if exists Contests

create table if not exists Contests (
    contest_id: UUID = UUID(),
    name: String(80),
    funds: Double = 2000.0,
    creationTime: DateTime = DateTime()
)

create index if not exists Contests#contest_id