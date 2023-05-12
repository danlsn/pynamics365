alter table staging.quotes
    alter column quoteid char(36) not null
go

alter table staging.quotes
    alter column modifiedon datetime not null
go

alter table staging.quotes
    add constraint quotes_pk
        primary key (modifiedon, quoteid)
go

