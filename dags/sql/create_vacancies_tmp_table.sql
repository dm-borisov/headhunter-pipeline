CREATE TABLE IF NOT EXISTS vacancies_{{ ds_nodash }}(
    id                 INT,
    name               VARCHAR(100),
    area_id            INT,
    employer_id        INT,
    published_at       TIMESTAMP,
    experience_id      VARCHAR(20),
    schedule_id        VARCHAR(20),
    professional_roles INT,
    alternate_url      VARCHAR(50),
    salary_from        INT,
    salary_to          INT,
    salary_currency    VARCHAR(10),
    salary_gross       BOOLEAN
);
