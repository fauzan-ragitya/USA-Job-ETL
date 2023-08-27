import requests
import os
import glob
import json
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import ast

class ExtractionHandler(object):
    def __init__(self):
        load_dotenv()
        authorization_key = os.getenv('API_AUTHORIZATION_KEY')
        api_host = os.getenv('API_HOST')
        root_url = os.getenv('API_ROOUT_URL')
        self.HEADERS = {'Host': api_host,'Authorization-Key': authorization_key}
        self.SEARCH_ENDPOINT = f'{root_url}search'
    
    def get_page_count(self,
                       params:dict):
        response = requests.get(self.SEARCH_ENDPOINT, headers=self.HEADERS, params=params).json()
        page_count = int(response['SearchResult']['UserArea']['NumberOfPages'])
        return page_count
    
    def get_raw_data(self, 
                     params:dict,
                     table_name:str,
                     execution_date:str) -> None:
        '''
        Send an HTTP request to the API. Create json files in data/extract/{table_name}/{execution_date} directory
        '''
        print(f'Beginning extract process for {table_name}')
        page_count = self.get_page_count(params=params)
        extract_dirname = os.path.join('data',table_name,execution_date,'extract')
        
        self._initialize_directory(dirname=extract_dirname)
        
        for i in range(1,page_count+1):
            params['page'] = i
            response = requests.get(self.SEARCH_ENDPOINT, headers=self.HEADERS, params=params).json()
            filename = os.path.join(extract_dirname,f'page_{i}.json')
            with open(filename,'w') as f:
                json.dump(response,f,indent=2)
        notes_filename = os.path.join(extract_dirname,'updated_at.txt') #contains last updated time of the extraction
        with open(notes_filename,'w') as outf:
            note = f'Extracted at {str(datetime.now())}'
            outf.write(note)

    def transform_data(self,
                       table_name:str,
                       execution_date:str,
                       custom_transformation_file:str) -> None:
        '''
        Read all json file from extract/ -> cleanse and transform the data
        '''
        print(f'Beginning transform process for {table_name}')
        extract_dirname = os.path.join('data',table_name,execution_date,'extract')
        transform_dirname = os.path.join('data',table_name,execution_date,'transform')
        self._initialize_directory(dirname=transform_dirname)

        #iterate through extract files
        extract_files = glob.glob(f'{extract_dirname}/*.json')
        for ext in extract_files:
            current_file = ext.split('/')[-1]
            with open(ext,'r') as ext_read:
                json_o = json.load(ext_read)

            search_result_items = json_o['SearchResult']['SearchResultItems']
            inputted_to_df = []
            for el in search_result_items:
                inputted_to_df.append(el['MatchedObjectDescriptor'])

            dataframe = pd.DataFrame.from_dict(inputted_to_df)
            code = ast.parse(open(custom_transformation_file).read())
            code_obj = compile(code, custom_transformation_file, mode='exec')

            _locals = locals()
            exec(code_obj, globals(), _locals)
            result = _locals['dataframe']
            transform_filename = os.path.join(transform_dirname,current_file)
            result.to_json(transform_filename,orient='records', indent=2)

            # current_file = ext.split('/')[-1]
            # with open(ext,'r') as ext_read:
            #     json_o = json.load(ext_read)

            # search_result_items = json_o['SearchResult']['SearchResultItems']
            # result = []
            # for el in search_result_items:
            #     matched_object_descriptor = el['MatchedObjectDescriptor']
            #     parsed_object = {}
            #     for field in parsed_fields:
            #         parsed_object[field] = matched_object_descriptor[field]
            #     result.append(parsed_object)

            # transform_filename = os.path.join(transform_dirname,current_file)
            # with open(transform_filename, 'w') as outputfile:
            #     json.dump(result,outputfile,indent=2)
                
        notes_filename = os.path.join(transform_dirname,'updated_at.txt') #contains last updated time of the extraction
        with open(notes_filename,'w') as outf:
            note = f'Transformed at {str(datetime.now())}'
            outf.write(note)

    def _initialize_directory(self,
                              dirname:str):
        '''
        Helper function to create a directory and make sure it is empty
        '''
        #make directory if not exists
        if not os.path.exists(dirname): 
            os.makedirs(dirname) 
        
        #remove files if exist
        files = glob.glob(f'{dirname}/*')
        for f in files:
            os.remove(f)
