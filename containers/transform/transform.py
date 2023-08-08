import jsonlines
from data_fields import vacancy_keys2
from flatten_json import flatten
from sqlalchemy import insert, select
from models import engine, skills_table, vacancies_table


def write_to_db(extract_path: str, table_name: str, engine):
    def get_data(func):
        def wrapper(*args, **kwargs):
            with (jsonlines.open(extract_path, mode='r') as reader,
                  engine.connect() as conn):

                for obj in reader:
                    data: dict = func(obj, *args, **kwargs)
                    if not data:
                        continue

                    conn.execute(insert(table_name), data)
                    conn.commit()

        return wrapper
    return get_data


@write_to_db("data/extracted2.jsonl", vacancies_table, engine)
def get_vacancies(obj: dict, keys: dict):
    flatten_obj = flatten(obj)

    vacancy = {}
    for key in keys:
        if key in flatten_obj.keys():
            vacancy[key] = flatten_obj[key]
        else:
            vacancy[key] = None

    return vacancy


@write_to_db("data/extracted2.jsonl", skills_table, engine)
def get_skills(obj: dict):
    return [{'id': obj['id'], 'skill': skill['name']}
            for skill in obj['key_skills']]


if __name__ == "__main__":

    get_skills()

    with engine.connect() as conn:
        stmt = select(skills_table)
        for row in conn.execute(stmt):
            print(row)

    get_vacancies(vacancy_keys2)

    with engine.connect() as conn:
        stmt = select(vacancies_table)
        for row in conn.execute(stmt):
            print(row)
