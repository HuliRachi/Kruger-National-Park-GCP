-- Full Load tables (accommodation, field_guides)

create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.accommodation`(
    AccID           string,
    CampID          string,
    UnitType        string,
    is_quarantined  boolean
);
truncate table  `project-a2ce378b-71f9-4087-95b.silver_dataset.accommodation`;

insert into `project-a2ce378b-71f9-4087-95b.silver_dataset.accommodation`
select distinct AccID, CampID, UnitType, 
        case 
            when AccID in null or CampID is null then TRUE
            else FALSE
        end as is_quarantined
from (
    select AccID, CampID, UnitType from 'project-a2ce378b-71f9-4087-95b.bronze_dataset.accommodation'
);
---------------------

create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.field_guides`(
    GuideID         string,
    FirstName       string,
    LastName        string,
    Specialization  string,
    CampID          string,
    PermitNo        string,
    is_quarantined  boolean
);

truncate table `project-a2ce378b-71f9-4087-95b.silver_dataset.field_guides`;

insert into `project-a2ce378b-71f9-4087-95b.silver_dataset.field_guides`
select distinct GuideID, FirstName, LastName, Specialization, CampID, PermitNo
case
    when GuideID is null or CampID is null the TRUE
    else FALSE
end as is_quarantined

from (
    select * from `project-a2ce378b-71f9-4087-95b.bronze_dataset.field_guides`
);

------------------


-- Incremental tables (billing, park_entries, visitors, september_free, kruger_gates)

create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.billing`(
    TransID         string,
    EntryID         string,
    VisitorID       string,
    GuideID         string,
    AccID           string,
    VisitDate       timestamp,
    ConservationFee float64,
    PaymentType     string,
    InsertDate      timestamp,
    modifiedDate    timestamp, --new
    is_current      boolean,
    is_quarantined  boolean

);

--create quality check temp table
create or replace table `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as
select distinct TransID, EntryID,
    VisitorID,
    GuideID,
    AccID,
    VisitDate,
    ConservationFee,
    PaymentType,
    InsertDate,
case
    when VisitorID is null or AccID is null then TRUE
    else FALSE
end as is_quarantined

from(
    select TransID, EntryID,
    VisitorID,
    GuideID,
    AccID,
    VisitDate,
    ConservationFee,
    PaymentType,
    InsertDate
    from `project-a2ce378b-71f9-4087-95b.bronze_dataset.billing`
);

--apply SCD type2
merge int `project-a2ce378b-71f9-4087-95b.silver_dataset.billing` as target
using `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as source
on target.VisitorID = source.VisitorID
and target.is_current = TRUE

--mark history data
when matched and (
    target.TransID <> source.TransID OR
    target.EntryID <> source.EntryID OR
    target.VisitorID <> source.VisitorID OR
    target.GuideID <> source.GuideID OR
    target.AccID <> source.AccID OR
    target.VisitDate <> source.VisitDate OR
    target.ConservationFee <> source.ConservationFee OR
    target.PaymentType <> source.PaymentType OR
    target.InsertDate <> source.InsertDate OR
    target.is_quarantined <> source.is_quarantined
)
then update set
    target.is_current = FALSE,
    target.InsertDate = CURRENT_TIMESTAMP()

--insert new and update record

when not matched
then insert(
    TransID,
    EntryID,
    VisitorID,
    GuideID,
    AccID,
    VisitDate,
    ConservationFee,
    PaymentType,
    InsertDate,
    modifiedDate,
    is_current,
    is_quarantined
)
values(
    source.TransID,
    source.EntryID,
    source.VisitorID,
    source.GuideID,
    source.AccID,
    source.VisitDate,
    source.ConservationFee,
    source.PaymentType,
    source.InsertDate,
    CURRENT_TIMESTAMP(),
    TRUE,
    source.is_quarantined
)
-- drop quality checks table
drop table if exists `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks`;

--PARK_ENTRIES TABLE 
create table if not exists  `project-a2ce378b-71f9-4087-95b.silver_dataset.park_entries`(
    EntryID         string,
    VisitorID       string,
    EntryDate       timestamp,
    ExitDate        timestamp,
    EntryGateID     string,
    AccID           string,
    VehicleReg      string,
    InsertedDate    timestamp,
    ModifiedDate    timestamp,
    is_quarantined  bool,
    is_current      bool
);

--create quality checks table
create or replace table `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks_park_entries`as
select * ,
case 
    when EntryID is null or VisitorID is null or AccID is null then TRUE
    else FALSE
end is_quarantined
from(
    select * from `project-a2ce378b-71f9-4087-95b.bronze_dataset.park_entries`
);
--apply SCD type 2

merge into `project-a2ce378b-71f9-4087-95b.silver_dataset.park_entries` as target
using `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks_park_entries` as source
on target.EntryID = source.EntryID
and target.is_current = TRUE

--mark existing record
when matched and (
    target.EntryID <> source.EntryID OR
    target.VisitorID <> source.VisitorID OR
    target.EntryDate <> source.EntryDate OR
    target.ExitDate <> source.ExitDate OR
    target.EntryGateID <> source.EntryGateID OR
    target.AccID <> source.AccID OR
    target.VehicleReg <> source.VehicleReg OR
    
    target.ModifiedDate <> source.ModifiedDate OR
    target.is_quarantined <> source.is_quarantined OR
    

)
then update set 
        target.is_current = FALSE
        target.ModifiedDate = CURRENT_TIMESTAMP()

--insert new  and update
when not matched
then insert(
    EntryID,
    VisitorID,
    EntryDate,
    ExitDate,
    EntryGateI,
    AccID,
    VehicleReg,
    InsertedDate,
    ModifiedDate,
    is_quarantined,
    is_current      
)

values(
    source.EntryID,
    source.VisitorID,
    source.EntryDate,
    source.ExitDate,
    source.EntryGateI,
    source.AccID,
    source.VehicleReg,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
    source.is_quarantined,
    TRUE 
);

--drop quality checks table
drop table if exists `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks_park_entries`

-- VISITORS TABLE

create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.visitors`(
    VisitorID       string,
    FirstName       string,
    LastName        string,
    ID_Passport_No  string,
    Nationality     string,
    WildCardMember  string,
    DOB             string,
    ContactNo       string,
    ModifiedDate    timestamp,
    InsertDate      timestamp,
    is_quarantined  bool,
    is_current      bool
);

--create a quality-checks temp table
create or replace table `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as
select * ,
    case
        when VisitorID is null or ID_Passport_No is null then TRUE
        else FALSE
    end is_quarantined
from(
    select * from `project-a2ce378b-71f9-4087-95b.bronze_dataset.visitors`
);

--apply scd type 2
merge into `project-a2ce378b-71f9-4087-95b.silver_dataset.visitors` as target
using `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as source
on target.VisitorID = source.VisitorID
and target.is_current = TRUE

--mark existing records as history
when matched and (
    target.VisitorID <> source.VisitorID OR
    target.FirstName <> source.FirstName OR
    target.LastName <> source.LastName OR
    target.ID_Passport_No <> source.ID_Passport_No OR
    target.Nationality <> source.Nationality OR
    target.WildCardMember <> source.WildCardMember OR
    target.DOB <> source.DOB OR
    target.ContactNo <> source.ContactNo OR
    
)
then update set
    target.is_current = FALSE,
    target.ModifiedDate = CURRENT_TIMESTAMP()

-- insert into new table
when not matched
then insert(
    VisitorID,
    FirstName,
    LastName,
    ID_Passport_No,
    Nationality,
    WildCardMember,
    DOB,
    ContactNo,
    ModifiedDate,
    InsertDate,
    is_quarantined,
    is_current      
)
values(
    source.VisitorID,
    source.FirstName,
    source.LastName,
    source.ID_Passport_No,
    source.Nationality,
    source.WildCardMember,
    source.DOB,
    source.ContactNo,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    source.is_quarantined,
    TRUE
)

--drop quality checks table
drop table if exists `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks`;

-- SEPTEMBER_FREE_ENTRY
create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.september_free_entry`(
    ClaimID         string,
    TransID         string,
    VisitorID       string,
    EntryID         string,
    CampID          string,
    EntryDate       timestamp,
    PayorType       string,
    AmountWaived    float64,
    InsertDate      timestamp,
    ModifiedDate    timestamp,
    is_quarantined  boolean,
    is_current      boolean

);

--create quality check table
create or replace table `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as
select *, 
    case
        when ClaimID is null then TRUE
        else FALSE
    end as is_quarantined
from(
    select * from `project-a2ce378b-71f9-4087-95b.bronze_dataset.september_free_entry`
);

--apply scd type2 
merge into `project-a2ce378b-71f9-4087-95b.silver_dataset.september_free_entry` as target
using `project-a2ce378b-71f9-4087-95b.quality_checks` as source
on target.CampID = source.CampID
and target.is_quarantined = TRUE

--mark existing record
when matched and (
    target.ClaimID <> source.ClaimID OR 
    target.TransID <> source.TransID OR
    target.VisitorID <> source.VisitorID OR
    target.EntryID <> source.EntryID OR
    target.CampID <> source.CampID OR
    target.EntryDate <> source.EntryDate OR
    target.PayorType <> source.PayorType OR
    target.AmountWaived <> source.AmountWaived OR
    target.InsertDate <> source.InsertDate OR
    target.is_quarantined <> source.is_quarantined
    
)
--insert new and update
when not matched
then insert(
    ClaimID,
    TransID,
    VisitorID,
    EntryID,
    CampID,
    EntryDate,
    PayorType,
    AmountWaived,
    InsertDate,
    ModifiedDate,
    is_quarantined,
    is_current      
)
values(
    source.ClaimID,
    source.TransID,
    source.VisitorID,
    source.EntryID,
    source.CampID,
    source.EntryDate,
    source.PayorType,
    source.AmountWaived,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    source.is_quarantined,
    TRUE  
);

--drop quality checks table
drop table if exists `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks`;

---KRUGER GATES
create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.kruger_gates`(
    GateID          string,
    GateName        string,
    KrugerCampName  string,
    ModifiedDate    timestamp,
    is_quarantined  boolean,
    is_current      boolean
);

--create quality checks table
create or replace table `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as
select *, 
    case
        when GateID is null then TRUE
        else FALSE
    end as is_quarantined
from(
    select * from `project-a2ce378b-71f9-4087-95b.bronze_dataset.kruger_gates`
);

--apply scd type 2
merge into `project-a2ce378b-71f9-4087-95b.silver_dataset.kruger_gates` as target
using `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as source
on target.GateID = source.GateID
and target.is_current = TRUE

--mark existing records
when matched and (
    target.GateID <> source.GateID OR
    target.GateName <> source.GateName OR
    target.KrugerCampName <> source.KrugerCampName OR
    target.is_quarantined <> source.is_quarantined
    
)

then update set
    target.is_current = FALSE,
    target.ModifiedDate = CURRENT_TIMESTAMP()

--insert new and update records 

when not matched
then insert(
    GateID,
    GateName,
    KrugerCampName,
    ModifiedDate,
    is_quarantined,
    is_current      
)
values(
    source.GateID,
    source.GateName,
    source.KrugerCampName,
    CURRENT_TIMESTAMP(),
    source.is_quarantined,
    TRUE  
);

--drop quality check table
drop table if exists `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks`