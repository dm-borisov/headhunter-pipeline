CREATE TABLE IF NOT EXISTS vacancies_load(
    id             INT,
    name           TEXT,
    alternate_url  TEXT,
    salary_from    INT,
    salary_to      INT,
    published_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skills_load(
    id    INT,
    skill TEXT
);
