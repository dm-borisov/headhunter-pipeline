CREATE TABLE IF NOT EXISTS companies_{{ ds_nodash }}(
    id                     INT,
    accredited_it_employer BOOLEAN,
    name                   VARCHAR(100),
    type                   VARCHAR(20),
    area_id                INT,
    site_url               VARCHAR(200),
    alternate_url          VARCHAR(200)
);
