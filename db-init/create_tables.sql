CREATE TABLE IF NOT EXISTS experience(
    id   VARCHAR(15) PRIMARY KEY,
    name VARCHAR(20) NOT NULL UNIQUE
);


CREATE TABLE IF NOT EXISTS schedule(
    id   VARCHAR(15) PRIMARY KEY,
    name VARCHAR(20) NOT NULL UNIQUE
);


CREATE TABLE IF NOT EXISTS areas(
    id        INT       PRIMARY KEY,
    parent_id INT,
    name      VARCHAR(100) NOT NULL
);


CREATE TABLE IF NOT EXISTS industries(
    id        VARCHAR(10) PRIMARY KEY,
    parent_id VARCHAR(10),
    name      VARCHAR(200) NOT NULL
);


CREATE TABLE IF NOT EXISTS companies(
    id                     INT          PRIMARY KEY,
    accredited_it_employer BOOLEAN      NOT NULL,
    name                   VARCHAR(200) NOT NULL,
    type                   VARCHAR(20)  NOT NULL,
    area_id                INT          NOT NULL,
    site_url               VARCHAR(200),
    url                    VARCHAR(200) NOT NULL,
    FOREIGN KEY (area_id) REFERENCES areas(id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS companies_to_industries(
    company_id  INT,
    industry_id VARCHAR(100),
    PRIMARY KEY (company_id, industry_id),
    FOREIGN KEY (company_id)  REFERENCES companies(id)  ON DELETE CASCADE,
    FOREIGN KEY (industry_id) REFERENCES industries(id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS vacancies(
    id                 INT          PRIMARY KEY,
    name               VARCHAR(100) NOT NULL,
    area_id            INT          NOT NULL,
    employer_id        INT          NOT NULL,
    published_at       TIMESTAMP    NOT NULL,
    experience_id      VARCHAR(20)  NOT NULL, 
    schedule_id        VARCHAR(20)  NOT NULL,
    professional_roles INT,
    url                VARCHAR(50)  NOT NULL,
    salary_from        INT,
    salary_to          INT,
    salary_currency    VARCHAR(10),
    salary_gross       BOOLEAN,
    FOREIGN KEY (area_id)       REFERENCES areas(id) ON DELETE CASCADE,
    FOREIGN KEY (employer_id)   REFERENCES companies(id)  ON DELETE CASCADE,
    FOREIGN KEY (experience_id) REFERENCES experience(id) ON DELETE CASCADE,
    FOREIGN KEY (schedule_id)   REFERENCES schedule(id)   ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS skills (
    id    SERIAL      PRIMARY KEY,
    skill VARCHAR(100) NOT NULL UNIQUE
)


CREATE TABLE IF NOT EXISTS vacancies_to_skills(
    vacancy_id INT,
    skill_id   INT,
    PRIMARY KEY (vacancy_id, skill_id),
    FOREIGN KEY (vacancy_id) REFERENCES vacancies (id) ON DELETE CASCADE,
    FOREIGN KEY (skill_id)   REFERENCES skills (id)    ON DELETE CASCADE
)
