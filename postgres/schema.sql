drop table if exists events;
create table events(
    id varchar(255) not null primary key,
    correlationid varchar(255) not null,
    sourceid varchar(255) not null,
    version int not null,
    eventtype varchar(50) not null,
    created timestamp not null,
    "event" json not null
);

drop table if exists events_integration;
create table events_integration(
    id varchar(255) not null primary key,
    correlationid varchar(255) not null,
    sourceid varchar(255) not null,
    version int not null,
    eventtype varchar(50) not null,
    created timestamp not null,
    "event" json not null
);

drop table if exists events_correlation;
create table events_correlation(
    id varchar(255) not null primary key,
    correlationid varchar(255) not null,
    sourceid varchar(255) not null,
    version int not null,
    eventtype varchar(50) not null,
    created timestamp not null,
    "event" json not null
);
create index events_correlation_idx on events_correlation(correlationid);

