drop if exists Participants

create table if not exists Participants (
    participant_id: UUID = UUID(),
    contest_id: UUID,
    member_id: UUID,
    funds: Double = 1000,
    creationTime: DateTime = DateTime()
)

create index if not exists Participants#contest_id

create index if not exists Participants#participant_id

create index if not exists Participants#member_id