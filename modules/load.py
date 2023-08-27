import os
from sqlalchemy import create_engine
import json
from dotenv import load_dotenv
import glob


class PostgresHandler(object):
    '''
    Object that will be used to handle destination table (PostgreSQL DB)
    '''
    def __init__(self) -> None:
        load_dotenv()
        db_name = os.getenv('POSTGRES_DB')
        # host = 'localhost' #use this when developping locally
        host = 'database' #use this to deployment on Docker container
        port = os.getenv('POSTGRES_PORT')
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        db_string = 'postgresql://{}:{}@{}:{}/{}'.format(
            user, password, host, port, db_name)
        self.conn = create_engine(db_string)

    def create_table(self, table_name: str, db_schema: list):
        '''
        Function to initialize empty table in DB
        '''
        # drop table first
        drop_query = f'DROP TABLE IF EXISTS {table_name};'
        self._execute_query(drop_query)

        print(f'Creating {table_name} table in DB')
        schema_declaration = []
        for field in db_schema:
            schema_declaration.append(
                f'{field["column_name"]} {field["type"]}')
        schema_declaration.append('loaded_time TIMESTAMP')
        schema_declaration = ','.join(schema_declaration)

        query = f'CREATE TABLE {table_name}({schema_declaration});'
        self._execute_query(query)

    def insert_data(self, table_name, execution_date, db_schema):
        '''
        Iteratively read through files in transform/ directory and insert it into Postgre DB
        '''
        print(f'Inserting data into table {table_name}')
        transform_dirname = os.path.join(
            'data', table_name, execution_date, 'transform')
        tranform_files = glob.glob(f'{transform_dirname}/*.json')
        error_count = 0 # to track error happening while inserting the data
        for trf_file in tranform_files:
            with open(trf_file, 'r') as f:
                json_o = json.load(f)
            for element in json_o:
                value_list = []
                for col in db_schema:
                    if col['type'] == 'JSON[]':
                        val = element[col['column_name']]
                        val = [json.dumps(el).replace("'","''") for el in val] # to handle if the JSON value contains single quotation mark (')
                        val = [f"'{el}'" for el in val]
                        val = ','.join(val)
                        val = f'array[{val}]::json[]'
                        # final format would be like: array['{"key1": "value1", "key2": "value2"}','{"key1":"another_val1",...}',...]::json[]
                        value_list.append(val)
                    elif col['type'] in ['VARCHAR', 'TEXT']:
                        val = str(element[col['column_name']])
                        val = f"'{val}'"
                        value_list.append(val)
                    else:
                        val = str(element[col['column_name']])
                        value_list.append(val)

                value_str = ','.join(value_list)
                query = f'''
                            INSERT INTO {table_name}
                            VALUES (
                                {value_str}, CURRENT_TIMESTAMP
                            );
                        '''
                try:
                    self._execute_query(query)
                except:
                    error_count += 1
                    with open('error_log.txt','a') as f: #will serve as collection of failed INSERT queries
                        f.write(query)
                        f.write()
        if error_count > 0:
            print(f'{error_count} errors happened while inserting the data. Please check error_log.txt file')
        else:
            print(f'Insert process is done!')

    def _execute_query(self, query):
        result = self.conn.execute(query)
        return result
