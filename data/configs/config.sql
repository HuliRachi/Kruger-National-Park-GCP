create table `project.....huli.temp_dataset.audit_log`( -- Project name temporary, use cloud project name
    tablename String,
    loadtype String,
    record_count int64,
    load_timestamp timestamp,
    status String
);