from faker import Faker
import psycopg2
import json
from datetime import datetime
from typing import Any, Dict

def read_config() -> dict:
    """read config file"""
    with open('conf.json', 'r', encoding='utf-8') as file:
        data: dict = json.load(file)
    return data

def create_connection() -> psycopg2.extensions.connection:
    """generate connection to postgresql"""
    return psycopg2.connect(
        host = conf['host'],
        dbname = conf['schema'],
        user = conf['login'],
        password = conf['psw'],
        port = '5432'
    )

def get_columns(schema: str, table: str) -> list:
    """get meta information of table (column and data type)"""
    data: list = []
    try:
        connect = create_connection()
        cursor = connect.cursor()
        query: str = """
            SELECT column_name,
                   data_type 
              FROM information_schema.columns
             WHERE table_name = %s
               AND table_schema = %s
        """
        cursor.execute(query, (table, schema))
        data = cursor.fetchall()
        cursor.close()
        connect.close()
        print(f'[{datetime.now()}] Get columns and data type from database')
    except Exception as error:
        print(f"[{datetime.now()}] Error: {error}")
    return data

def generate_value(fake: Faker, data_type: str) -> Any:
    """generate a value on depending of data type from handler dictionary"""
    type_handlers: Dict[str, Any] = {
        'bigint': lambda: fake.random.randint(100, 10000),
        'smallint': lambda: fake.random.randint(100, 10000),
        'integer': lambda: fake.random.randint(100, 10000),
        'varchar': lambda: fake.word(),
        'char': lambda: fake.word(),
        'text': lambda: fake.text(),
        'timestamp without time zone': lambda: fake.date(),
        'date': lambda: fake.date(),
        'boolean': lambda: fake.boolean(),
        'double': lambda: fake.pyfloat(),
        'float': lambda: fake.pyfloat(),
        'decimal': lambda: fake.pyfloat(),
        'numeric': lambda: fake.pyfloat()
    }
    data_type_lower = data_type.lower()
    handler = type_handlers.get(data_type_lower)
    return handler() if handler else None

def generate_synthetic_data(columns: list, series: int) -> list:
    """generate synthetic information for current table"""
    fake: Faker = Faker('ru_RU')
    synthetic_data: list = []
    for _ in range(series):
        record: list = []
        for column_name, data_type in columns:
            value: Any = generate_value(fake, data_type)
            record.append(value)
        synthetic_data.append(tuple(record))
    print(f'[{datetime.now()}] Generated {series} synthetic records')
    return synthetic_data

def insert_data(synthetic_data: list, columns: list) -> None:
    """add records to the database"""
    if not synthetic_data:
        return
    try:
        connect = create_connection()
        cursor = connect.cursor()
        col_names: list = [col[0] for col in columns]
        placeholders: str = ', '.join(['%s'] * len(columns))
        query: str = f"INSERT INTO {conf['table']} ({', '.join(col_names)}) VALUES ({placeholders})"
        cursor.executemany(query, synthetic_data)
        connect.commit()
        cursor.close()
        connect.close()
        print(f'[{datetime.now()}] Added {len(synthetic_data)} records to the database')
    except Exception as error:
        print(f"[{datetime.now()}] Error: {error}")

conf: dict = read_config()
tmp: list = conf['table'].split('.')
schema: str = tmp[0]
table: str = tmp[1]
columns: list = get_columns(schema, table)
synthetic: list = generate_synthetic_data(columns, conf['series'])
insert_data(synthetic, columns)