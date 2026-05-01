create table accommodation(
    AccID           nvarchar(50)  not null,
    CampID          nvarchar(50)  not null,
    UnitType        nvarchar(50)  not null,
    MaxOccupancy    int           not null,
    BasePrice       money         not null,
    constraint pk_accommodation primary key (AccID)
);

create table billing(
    TransID             nvarchar(50)    not null,
    EntryID             nvarchar(50)    not null,
    VisitorID           nvarchar(50)    not null,
    GuideID             nvarchar(50),
    AccID               nvarchar(50)    not null,
    VisitDate           date            not null,
    ConservationFee     money           not null,
    AccommFee           money           not null,
    ActivityFee         money           not null,
    TotalAmount         money           not null,
    PaymentType         nvarchar(50)    not null,
    InsertDate          datetime2        not null,
    constraint pk_billing primary key (TransID)
);

create table field_guides(
    GuideID         nvarchar(50)    not null,
    FirstName       nvarchar(50)    not null,
    LastName        nvarchar(50)    not null,
    Specialization  nvarchar(50)    not null,
    CampID          nvarchar(50)    not null,
    PermitNo        nvarchar(50)    not null,
    constraint pk_field_guides primary key (GuideID)    
);

create table park_entries(
    EntryID        nvarchar(50)     not null,
    VisitorID      nvarchar(50)     not null,
    EntryDate      datetime2        not  null,
    ExitDate       datetime2        not null,
    EntryGateID    nvarchar(20)     not null,
    AccID          nvarchar(20)     not null,
    VehicleReg     nvarchar(50)     not null,
    InsertedDate   datetime2        not null,
    ModifiedDate   datetime2        not null,
    constraint pk_park_entries primary key (EntryID)
);

create table visitors(
    VisitorID       nvarchar(50)        not null,
    FirstName       nvarchar(30)        not null,
    LastName        nvarchar(30)        not null,
    ID_Passport_No  nvarchar(50)        not null,
    Nationality     nvarchar(50)        not null,
    WildCardMember  char(1)             not null,
    DOB             date                not null,
    ContactNo       nvarchar(30)        not null,
    ModifiedDate    datetime2           not null,
    constraint pk_visitors primary key (VisitorID)
);