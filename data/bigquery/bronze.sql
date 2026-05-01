-- Project name temporary, use the one on cloud

create external table if not exists `project.....huli.bronze_dataset.accommodation`
options(
    format = 'JSON',
    uris = ['gs://............................./*json']
);

create external table if not exists `project.....huli.bronze_dataset.billing`
options(
    format = 'JSON',
    uris = ['gs://............................./*json']
);

create external table if not exists `project.....huli.bronze_dataset.field_guides`
options(
    format = 'JSON',
    uris = ['gs://............................./*json']
);

create external table if not exists `project.....huli.bronze_dataset.park_entries`
options(
    format = 'JSON',
    uris = ['gs://............................./*json']
);

create external table if not exists `project.....huli.bronze_dataset.visitors`
options(
    format = 'JSON',
    uris = ['gs://............................./*json']
);