
create table accommodation(
    AccID           varchar(50)  not null,
    CampID          varchar(50)  not null,
    UnitType        varchar(50)  not null,
    MaxOccupancy    int          not null,
    BasePrice       decimal(10,2) not null,
    constraint pk_accommodation primary key (AccID)
);

create table billing(
    TransID             varchar(50)    not null,
    EntryID             varchar(50)    not null,
    VisitorID           varchar(50)    not null,
    GuideID             varchar(50),
    AccID               varchar(50)    not null,
    VisitDate           date           not null,
    ConservationFee     decimal(10,2)  not null,
    AccommFee           decimal(10,2)  not null,
    ActivityFee         decimal(10,2)  not null,
    TotalAmount         decimal(10,2)  not null,
    PaymentType         varchar(50)    not null,
    InsertDate          TIMESTAMP      not null,
    constraint pk_billing primary key (TransID)
);


create table field_guides(
    GuideID         varchar(50)    not null,
    FirstName       varchar(50)    not null,
    LastName        varchar(50)    not null,
    Specialization  varchar(50)    not null,
    CampID          varchar(50)    not null,
    PermitNo        varchar(50)    not null,
    constraint pk_field_guides primary key (GuideID)    
);

create table park_entries(
    EntryID        varchar(50)     not null,
    VisitorID      varchar(50)     not null,
    EntryDate      TIMESTAMP        not  null,
    ExitDate       TIMESTAMP        not null,
    EntryGateID    varchar(20)     not null,
    AccID          varchar(20)     not null,
    VehicleReg     varchar(50)     not null,
    InsertedDate   TIMESTAMP        not null,
    ModifiedDate   TIMESTAMP        not null,
    constraint pk_park_entries primary key (EntryID)
);

create table visitors(
    VisitorID       varchar(50)        not null,
    FirstName       varchar(30)        not null,
    LastName        varchar(30)        not null,
    ID_Passport_No  varchar(50)        not null,
    Nationality     varchar(50)        not null,
    WildCardMember  char(1)             not null,
    DOB             date                not null,
    ContactNo       varchar(30)        not null,
    ModifiedDate    TIMESTAMP           not null,
    constraint pk_visitors primary key (VisitorID)
);