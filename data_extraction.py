import boto3
import pandas as pd
import requests
import tabula
import yaml

class DataExtractor:
    # read the API credentials/URLs
    def read_api_creds(self):
        '''
        Loads configuration for the non-database endpoints from a file (api_creds.yaml).
            Parameters:
                    none.
            Returns:
                    config_data (dictionary): The values for external API calls and URLs for data extraction.
        '''
        with open('api_creds.yaml', 'r') as file:
            config_data = yaml.safe_load(file)
        return config_data

    # extract the database table to a pandas DataFrame
    def read_rds_table(self, db_connector, table_name):
        '''
        Reads a database table into a Pandas dataframe.
            Parameters:
                    db_connector: Instance of a DataConnector class to use for database access.
                    table_name: the table in the database to retieve data from.
            Returns:
                    table_data (Pandas dataframe): The values for establishing source(RDS) and target(local) database connections.
        '''
        db_creds = db_connector.read_db_creds()
        db_engine = db_connector.init_db_engine(db_creds)
        data_frame = pd.read_sql_table(table_name, db_engine)
        return data_frame

    # returns a Pandas DataFrame from a link to a PDF.
    # Note this uses Tabula which requires Java/JRE to be installed
    def retrieve_pdf_data(self, url, pages='all'):
        '''
        Loads tables in a PDF into a dataframe.
            Parameters:
                    url - URL to a PDF to read with table data.
            Returns:
                    pdf_data (Pandas dataframe): A table with the table data. 
        '''
        pdf_data = tabula.read_pdf(url, pages=pages)
        return pd.concat(pdf_data)

    # returns the number of stores to extract
    def list_number_of_stores(self, api_header_dict, number_stores_endpoint_url):
        '''
        Returns the number of stores
            Parameters:
                    api_header_dict (dictionary): API headers.
                    number_stores_endpoint_url: The endpoint to read the store count from.
            Returns:
                    config_data (dictionary): The values for establishing source(RDS) and target(local) database connections.
        '''
        response = requests.get(number_stores_endpoint_url, headers=api_header_dict)
        return response.json()["number_stores"]
    
    # extracts all stores from the API saving them in a pandas DataFrame
    def retrieve_stores_data(self, api_header_dict, store_data_endpoint_template, max_store_number):
        '''
        Returns the details for stores up to max_store_number.
            Parameters:
                    api_header_dict (dictionary): API headers.
                    store_data_endpoint_template (str): The endpoint to read the store details from.
                    max_store_number (int): The maximum store number to read up to. This is 0 based.
            Returns:
                    store_data (Pandas dataframe): The all the store details from the API.
        '''
        store_data_list = self.retrieve_stores_data_range(api_header_dict, store_data_endpoint_template, max_store_number, 0)
        return pd.concat(store_data_list)
    
    # extracts a range of stores from the API saving them in a list
    # this can be used to extract a subset of stores for use in a multi-threaded download or to allow for the process to be interrupted or monitored
    def retrieve_stores_data_range(self, api_header_dict, store_data_endpoint_template, max_store_number, min_store_number = 0):
        '''
        Returns the details for stores from min_store_number up to max_store_number.
            Parameters:
                    api_header_dict (dictionary): API headers.
                    store_data_endpoint_template (str): The endpoint to read the store details from.
                    max_store_number (int): The maximum store number to read up to. This is 0 based.
                    min_store_number (int) (optional): Start reading from this store_number. Note this is 0 based.
            Returns:
                    store_data_list (list): The all the store details from the API.
        '''
        store_data_list = []
        for store_number in range(min_store_number, max_store_number):
            store_data_endpoint_url = store_data_endpoint_template.format(store_number=store_number)
            response = requests.get(store_data_endpoint_url, headers=api_header_dict)
            store_data_list.append(pd.DataFrame(response.json(), index=["index"]))
        return store_data_list
    
    # download from s3 and extract the information returning a pandas DataFrame
    def extract_from_s3(self, s3uri):
        '''
        Loads a table from a CSV in AWS S3.
            Parameters:
                    s3uri - URI to the CSV in S3.
            Returns:
                    pdf_data (Pandas dataframe): A table with the data. 
        '''
        s3uri_split = s3uri.split('/')
        s3client = boto3.client('s3')
        s3response = s3client.get_object(Bucket=s3uri_split[2], Key=s3uri_split[3])
        return pd.read_csv(s3response.get('Body'))

    # returns the number of stores to extract
    def extract_from_json(self, url):
        '''
        Loads tables in a PDF into a dataframe.
            Parameters:
                    url - URL to a JSDON file to read table data from.
            Returns:
                    data (Pandas dataframe): A table with the JSON data. 
        '''
        return pd.read_json(url)    
