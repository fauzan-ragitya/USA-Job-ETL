from datetime import datetime
import yaml
from modules.extraction import ExtractionHandler
from modules.load import PostgresHandler
def destination_config_handler() -> dict:
    '''
    Helper function to read destination_config.yml and convert it to dictionary
    '''
    config_filename = 'destination_config.yml'
    with open(config_filename, 'r') as f:
        dest_config = yaml.load(f, Loader=yaml.FullLoader)
    return dest_config

if __name__ == '__main__':
    execution_time = datetime.now()
    print(f'ETL process starts in {execution_time}')
    execution_date = str(execution_time.date())
    extractor_object = ExtractionHandler()
    db_handler_object = PostgresHandler()

    dest_config = destination_config_handler()
    for dest_table in dest_config['tables']:

        table_name = dest_table['name']
        params = dest_table['params']
        db_schema = dest_table['db_schema']
        custom_transformation_file = dest_table['custom_transformation_file']

        print(f'Processing table {table_name}')
        extractor_object.get_raw_data(
            params=params, table_name='data_engineering_jobs', execution_date=execution_date)
        extractor_object.transform_data(table_name='data_engineering_jobs', execution_date=execution_date,
                                        custom_transformation_file=custom_transformation_file)

        db_handler_object.create_table(
            table_name=table_name, db_schema=db_schema)
        db_handler_object.insert_data(
            table_name=table_name, execution_date=execution_date,db_schema=db_schema)
