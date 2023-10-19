create table students (name String(64), age INT, gpa DECIMAL(3, 2))
  clustered by (age) into 2 BUCKETS STORED as ORC;

insert into table students
  values ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);


create table pageviews (userid String(64), link String, came_from String)
  partitioned by (datestamp String) clustered by (userid) into 256 BUCKETS STORED as ORC;

insert into table pageviews PARTITION (datestamp = '2014-09-23')
  values ('jsmith', 'mail.com', 'sports.com'), ('jdoe', 'mail.com', null);

insert into table pageviews PARTITION (datestamp)
  values ('tjohnson', 'sports.com', 'finance.com', '2014-09-23'), ('tlee', 'finance.com', null, '2014-09-21');

insert into table pageviews
  values ('tjohnson', 'sports.com', 'finance.com', '2014-09-23'), ('tlee', 'finance.com', null, '2014-09-21');