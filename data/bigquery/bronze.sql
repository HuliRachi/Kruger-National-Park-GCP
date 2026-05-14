-- Project name temporary, use the one on cloud

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.accommodation`
options(
    format = 'JSON',
    uris = ['gs://kruger-bucket/landing/kruger-national-park/accommodation/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.billing`
options(
    format = 'JSON',
    uris = ['gs://kruger-bucket/landing/kruger-national-park/billing/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.field_guides`
options(
    format = 'JSON',
    uris = ['gs://kruger-bucket/landing/kruger-national-park/field_guides/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.park_entries`
options(
    format = 'JSON',
    uris = ['gs://kruger-bucket/landing/kruger-national-park/park_entries/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.visitors`
options(
    format = 'JSON',
    uris = ['gs://kruger-bucket/landing/kruger-national-park/visitors/*json']
);