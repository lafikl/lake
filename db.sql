CREATE TABLE lake_records (
    "id" serial primary key,
    "uid" UUID NOT NULL,
    "namespace" varchar(100) NOT NULL,
    "metadata" jsonb NOT NULL,
    "blob" jsonb NOT NULL
);

INSERT INTO lake_records (namespace, metadata, blob) VALUES ('ff', '{"a":1}', '{"a":1}');