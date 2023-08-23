# :bulb: headhunter-pipeline

## :page_facing_up: Description
The main purpose of this project is to help with data gathering from a Head-Hunter API for further analysis. The extracted data can be used in different ways:
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
It's an ETL process. Data is extracted from the Head-Hunter API, transformed by python script and loaded into the `PostgreSQL` database.

The database schema looks like this (made in [dbdiagram](https://dbdiagram.io/d)):
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/d9c9c374-fbfd-4848-976c-7b9ff8a49a62)

There are three dags to populate it:
1. `aux_tables_dag` downloads next auxiliary tables: areas, industries, experience, schedule. It should be run first, because other tables are dependent on them
2. `companies_dag` is an ETL process that populates the next tables: companies, companies_to_industries
3. `vacancies_dag` is an ETL process that populates the remaining tables: vacancies, skills, vacancies_to_skills. Because of dependencies, it should run after `companies dag`

Both `companies_dag` and `vacancies_dag` can only have one `dag_instance` running at a time. If there is more `dag_instances`, then you can overwhelm the API and get Captcha.
 
Here's the `companies_dag` graph:
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/5513aa51-822f-4889-960c-079ec78cf9ca)

And the `vacancies_dag` graph here:
![image](https://github.com/dm-borisov/headhunter-pipeline/assets/141336932/98171735-8ac7-4a9f-9028-cd5ee11ba385)

They have similar steps besides the `wait_for_companies_dag` sensor. The vacancy table is dependent on the company table, so it's necessary to load data into the latter one first.

There are five steps that consist of multiple tasks grouped by TaskGroups:
1. Create. This step creates temporary tables for transformed data by using `PostgresOperator`
2. Extract. In this step, a python script wrapped in a Docker container pulls data from the Head-Hunter API and stores it in jsonlines-file. Delay between queries is implemented to avoid getting Captcha
3. Transform. Here the data is transformed by python script which is also wrapped in a Docker container and stored in temporary tables from step 1
4. Load. This is a bunch of `PostgresOperator` that insert data from temporary tables into main tables. Duplicates in temporary tables and between temporary tables and main tables are checked
5. Clean. The last step has `BashOperator` to remove jsonlines-file and two `PostgresOperator` to drop temporary tables
