from faker import Faker
import psycopg2
import json
from datetime import datetime

def read_conf()->dict:
    with open('conf.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def get_columns(shema:str, table:str) -> []:
    data:list = []
    try:
        connect = psycopg2.connect(host = conf['host'], dbname = conf['schema'], user =  conf['login'], password =  conf['psw'], port = '5432')
        cursor = connect.cursor()
        query = f"""SELECT column_name,
                        data_type 
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                      AND table_schema = '{schema}'
                """
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        connect.close()
        print(f'[{datetime.now()}] Get columns and data type from database')
    except Exception as error:
        print(error)
    return data

def generate_synthetic_data(columns:list, series:int) -> list:
    fake = Faker('ru_RU')
    synthetic_data = []
    query:str = 'VALUES ('
    for i in range(0, series):
        for idx in range(0, len(columns)):
            if idx == 0:
                if columns[idx][1] in ['bigint', 'smallint', 'integer']:
                    query += f'{fake.random.randint(100, 1000)}'
                elif columns[idx][1] in ['varchar', 'char', 'text']:
                    query += f"'{fake.word()}'"
                elif columns[idx][1] in ['timestamp', 'date']:
                    query += f"'{fake.iso8601()}'"
                elif columns[idx][1] in ['boolean']:
                    query += f'{fake.boolean()}'
                elif columns[idx][1] in ['double', 'float', 'decimal']:
                    query += f'{fake.pyfloat()}'
                else:
                    query += f'NULL'   
            else:
                if columns[idx][1] in ['bigint', 'smallint', 'integer']:
                    query += f',{fake.random.randint(100, 1000)}'
                elif columns[idx][1] in ['varchar', 'char', 'text']:
                    query += f",'{fake.word()}'"
                elif columns[idx][1] in ['timestamp', 'date']:
                    query += f",'{fake.iso8601()}'"
                elif columns[idx][1] in ['boolean']:
                    query += f',{fake.boolean()}'
                elif columns[idx][1] in ['double', 'float', 'decimal']:
                    query += f',{fake.pyfloat()}'
                else:
                    query += f',NULL'
        query += ')'
        synthetic_data.append(query)
        query = ', ('
    print(f'[{datetime.now()}] Generated synthetic data')
    return synthetic_data

def insert_data(synthetic_data:list, columns:list):
    query_insert:str = f'INSERT INTO {conf['table']} ('
    for idx in range(0, len(columns)):
        if idx == len(columns) - 1:
            query_insert += f'{columns[idx][0]})'
        else:
            query_insert += f'{columns[idx][0]},'
    for row in synthetic_data:
        query_insert += row
    try:
        connect = psycopg2.connect(host = conf['host'], dbname = conf['schema'], user =  conf['login'], password =  conf['psw'], port = '5432')
        cursor = connect.cursor()
        cursor.execute(query_insert)
        connect.commit()
        connect.close()
        print(f'[{datetime.now()}] Added synthetic data to the database')
    except Exception as error:
        print(error)

conf = read_conf()
tmp:list = conf['table'].split('.')
schema = tmp[0]
table = tmp[1]
columns:list = get_columns(schema, table)
synthetic = generate_synthetic_data(columns, conf['series'])
insert_data(synthetic, columns)