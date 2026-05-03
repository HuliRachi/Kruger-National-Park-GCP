-- Full Load tables (accommodation, field_guides)

create table if not exists `project.....huli.silver_dataset.accommodation`(
    AccID           string,
    CampID          string,
    UnitType        string,
    is_quarantined  boolean
);
truncate table  `project.....huli.silver_dataset.accommodation`;

insert into `project.....huli.silver_dataset.accommodation`
select distinct AccID, CampID, UnitType, 
        case 
            when AccID in null or CampID is null then TRUE
            else FALSE
        end as is_quarantined
from (
    select AccID, CampID, UnitType from 'project.....huli.bronze_dataset.accommodation'
);
---------------------

create table if not exists `project.....huli.silver_dataset.field_guides`(
    GuideID         string,
    FirstName       string,
    LastName        string,
    Specialization  string,
    CampID          string,
    PermitNo        string,
    is_quarantined  boolean
);

truncate table `project....huli.silver_dataset.field_guides`;

insert into `project....huli.silver_dataset.field_guides`
select distinct GuideID, FirstName, LastName, Specialization, CampID, PermitNo
case
    when GuideID is null or CampID is null the TRUE
    else FALSE
end as is_quarantined

from (
    select * from `project....huli.bronze_dataset.field_guides`
);

------------------


-- Incremental tables (billing, park_entries, visitors, september_free, kruger_gates)

create table if not exists `project.....huli.silver_dataset.billing`(
    TransID string,
    EntryID string,
    VisitorID string,
    GuideID string,
    AccID string,
    VisitDate string,
    ConservationFee float64,
    PaymentType string,
    InsertDate timestamp,
    modifiedDate timestamp, --new
    is_current boolean,
    is_quarantined boolean

);

--create quality check temp table
create or replace table `project.....huli.silver_dataset.quality_checks` as
select distinct TransID, EntryID,
    VisitorID,
    GuideID,
    AccID,
    VisitDate,
    ConservationFee,
    PaymentType,
    InsertDate,
case
    when VisitorID is null or GuideID is null or AccID is null then TRUE
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
    from `project....huli.bronze_dataset.billing`
);

--apply SCD type2
merge int `project.....huli.silver_dataset.billing` as target
using `project.....huli.silver_dataset.quality_checks` as source
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
drop table if exists `project.....huli.silver_dataset.quality_checks`;

--PARK_ENTRIES TABLE 
create table if not exists  `project.....huli.silver_dataset.park_entries`(
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
create or replace table `project.....huli.silver_dataset.quality_checks_park_entries`as
select * ,
case 
    when EntryID is null or VisitorID is null or AccID is null then TRUE
    else FALSE
end is_quarantined
from(
    select * from `project.....huli.bronze_dataset.park_entries`
);
--apply SCD type 2

merge into `project.....huli.silver_dataset.park_entries` as target
using `project.....huli.silver_dataset.quality_checks_park_entries` as source
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
drop table if exists `project.....huli.silver_dataset.quality_checks_park_entries`

-- VISITORS TABLE

