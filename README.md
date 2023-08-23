# :bulb: headhunter-pipeline

## :page_facing_up: Description
The main purpose of this project is to help with data gathering from a head-hunter API for further analysis. The extracted data can be used in different ways:
- job searching;
- finding tendencies in the job market;
- finding insights for specific jobs that are not mentioned in public reports;
- and more...

## :toolbox: Build with
<div>
  <img alt="Airflow" src="https://github.com/dm-borisov/dm-borisov/assets/141336932/ee6ac6d5-a8f5-4e1c-aae1-4debabd711dc" height=40 width=40 />
  <img alt="PostgreSQL" src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" height=40 width=40 />
  <img alt="Docker" src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" height=40 width=40 />
</div>

## :construction: Installation
1. Clone the repository
```sh
git clone https://github.com/dm-borisov/headhunter-pipeline.git
```
2. Add environment variables to `.env` files inside of `aux_extract` and `extract` containers. Here's an example with default parameters:
```sh
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres
CONN_URL=172.17.0.1
```
3. Run docker compose
```sh
docker compose up
```
4. Add `PostgreSQL` connection to `Apache Airflow`. Default example:
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/2f04683c-4daa-4649-ab8c-ebc3d389d378)
5. Add sequential pool
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/ce39c1cc-c6b5-4027-bb92-ccec54137944)
6. Run `aux_tables_dag` to download auxiliary tables
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/557ba05a-2212-4b36-b09c-ba4a530e3fa0)

## :gear: How it works
It's a standard ETL process. Extract and transform are written in `python` and are wrapped in Docker containers. Load phase is written in `sql`.

In my case, there are three dags:
1. `aux_tables_dag` downloads next auxiliary tables: areas, industries, experience, schedule
2. `companies_dag` is an ETL process for companies that published vacancies yesterday
3. `vacacnies_dag` is an ETL process for yesterday vacancies

Due to dependencies it's necessary to run `aux_tables_dag` before activating other dags. Also `companies_dag` should be run before `vacancies_dag`. Here's the project database schema (made in [dbdiagram](https://dbdiagram.io/d)):
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/d9c9c374-fbfd-4848-976c-7b9ff8a49a62)

The whole process takes time because of API restriction. You can easily get Captcha if you try to load with minimal delays or in parallel. To implement restrictions I did:
1. API delay to between 0.5 to 1 minute in extract script
2. Restrict dag's run to 1
3. Make `vacancies_dag` run only after `companies_dag` completed (also tables dependencies is also a reason)

Here's `companies_dag` graph:
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/5513aa51-822f-4889-960c-079ec78cf9ca)

And `vacancies_dag` graph here:
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/98171735-8ac7-4a9f-9028-cd5ee11ba385)

They have similar steps besides `wait_for_companies_dag` sensor. Vacancies table are dependent on Companies tables, so it's necessary to load data in latter one first.

The whole process is made of 5 steps represented by TaskGroups:
1. Create. In this step temporary tables for transformed data are created.
2. Extract. In this step the data are extracted from headhunter-api. To get around limit of 2000 items per query, I divided one query into three by different experience. You can also do it by using different queries or areas. The data are stored in jsonlines-file.
4. Transform. In this step the data from jsonlines-file are transformed and stored into temporary tables.
5. Load. In this step the data are inserted in main tables. Duplicates inside temporary table (for example, companies can occur multiple times) and between temporary table and main table are checked.
6. Clean. This is the final step where jsonfiles and temporary tables are deleted.
