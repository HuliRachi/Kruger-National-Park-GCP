-- Project name temporary, use the one on cloud

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.accommodation`
options(
    format = 'JSON',
    uris = ['gs://kruger-national-park-bucket/landing-folder/kruger/accommodation/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.billing`
options(
    format = 'JSON',
    uris = ['gs://kruger-national-park-bucket/landing-folder/kruger/billing/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.field_guides`
options(
    format = 'JSON',
    uris = ['gs://kruger-national-park-bucket/landing-folder/kruger/field_guides/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.park_entries`
options(
    format = 'JSON',
    uris = ['gs://kruger-national-park-bucket/landing-folder/kruger/park_entries/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.visitors`
options(
    format = 'JSON',
    uris = ['gs://kruger-national-park-bucket/landing-folder/kruger/visitors/*json']
);