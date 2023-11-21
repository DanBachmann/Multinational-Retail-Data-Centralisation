import pandas as pd
import requests
import boto3
import tabula
import yaml

class DataExtractor:
    def read_api_creds(self):
        with open('api_creds.yaml', 'r') as file:
            config_data = yaml.safe_load(file)
        return config_data

    # extract the database table to a pandas DataFrame
    def read_rds_table(self, db_connector, table_name):
        db_creds = db_connector.read_db_creds()
        db_engine = db_connector.init_db_engine(db_creds)
        data_frame = pd.read_sql_table(table_name, db_engine)
        return data_frame

    # returns a Pandas DataFrame from a link to a PDF
    def retrieve_pdf_data(self, url, pages='all'):
        pdf_data = tabula.read_pdf(url, pages=pages)
        return pd.concat(pdf_data)

    # returns the number of stores to extract
    def list_number_of_stores(self, api_header_dict, number_stores_endpoint_url):
        response = requests.get(number_stores_endpoint_url, headers=api_header_dict)
        return response.json()["number_stores"]
    
    # retrieve_stores_data which will take the retrieve a store endpoint as an argument and extracts all the stores from the API saving them in a pandas DataFrame.
    def retrieve_stores_data(self, api_header_dict, store_data_endpoint_template, number_of_stores, console_log=False):
        store_data_list = []
        for store_number in range(0, number_of_stores):
            store_data_endpoint_url = store_data_endpoint_template.format(store_number=store_number)
            response = requests.get(store_data_endpoint_url, headers=api_header_dict)
            store_data_list.append(pd.DataFrame(response.json(), index=["index"]))
            if console_log:
                print(f"\r reading store {store_number+1}/{number_of_stores}                                         ", end="", flush=True)
        if console_log:
            print("\r                                         ", end="")
        return pd.concat(store_data_list)
    
    # download from s3 and extract the information returning a pandas DataFrame
    def extract_from_s3(self, s3uri):
        s3uri_split = s3uri.split('/')
        s3client = boto3.client('s3')
        s3response = s3client.get_object(Bucket=s3uri_split[2], Key=s3uri_split[3])
        return pd.read_csv(s3response.get('Body'))

    # returns the number of stores to extract
    def extract_from_json(self, data_url):
        return pd.read_json(data_url)    
