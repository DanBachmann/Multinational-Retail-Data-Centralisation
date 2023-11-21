import unittest
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

class TestDataCleaning(unittest.TestCase):
    # def test_clean_rds_table(self):
    #     db_connector = DatabaseConnector()
    #     db_creds = db_connector.read_db_creds()
    #     db_engine = db_connector.init_db_engine(db_creds)
    #     table_names = db_connector.list_db_tables(db_engine)
    #     for table_name in table_names:
    #         data_extractor = DataExtractor()
    #         data_frame = data_extractor.read_rds_table(db_connector, table_name)
    #         data_cleaning = DataCleaning()
    #         clean_data_frame = data_cleaning.clean_user_data(data_frame)
    #         print(table_name)
    #         print(clean_data_frame.info())
    #         self.assertTrue(type(clean_data_frame),type(pd.DataFrame()))
    
    # def test_clean_card_data(self):
    #     data_extractor = DataExtractor()
    #     data_cleaning = DataCleaning()
    #     api_config = data_extractor.read_api_creds()
    #     card_data_url = api_config['card_data_url']
    #     data_frame = data_extractor.retrieve_pdf_data(card_data_url)
    #     clean_data_frame = data_cleaning.clean_card_data(data_frame)
    #     self.assertTrue(type(clean_data_frame),type(pd.DataFrame()))
    
    # def test_clean_stores_data(self):
    #     data_extractor = DataExtractor()
    #     data_cleaning = DataCleaning()
    #     api_config = data_extractor.read_api_creds()
    #     api_header_dict = {'x-api-key': api_config['x-api-key']}
    #     store_data_template = api_config['store_data_template']
    #     data_frame = data_extractor.retrieve_stores_data(api_header_dict, store_data_template, 3, True)
    #     clean_data_frame = data_cleaning.called_clean_store_data(data_frame)
    #     self.assertTrue(type(clean_data_frame),type(pd.DataFrame()))

    def test_clean_product_weights(self):
        data_extractor = DataExtractor()
        data_cleaning = DataCleaning()
        api_config = data_extractor.read_api_creds()
        products_csv_uri = api_config['products_csv_uri']
        products_data_frame = data_extractor.extract_from_s3(products_csv_uri)
        clean_data_frame = data_cleaning.convert_product_weights(products_data_frame)
        self.assertTrue(type(clean_data_frame),type(pd.DataFrame()))

if __name__ == '__main__':
    unittest.main()
