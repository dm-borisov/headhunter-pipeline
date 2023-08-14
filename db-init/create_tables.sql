CREATE TABLE IF NOT EXISTS vacancies_tmp(
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

CREATE TABLE IF NOT EXISTS skills_tmp(
    id    INT,
    skill VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS companies_tmp(
    id                     INT,
    accredited_it_employer BOOLEAN,
    name                   VARCHAR(100),
    type                   VARCHAR(20),
    area_id                INT,
    site_url               VARCHAR(200),
    alternate_url          VARCHAR(200)
);


CREATE TABLE IF NOT EXISTS industries_tmp(
    id        INT,
    indsustry VARCHAR(100)

);
