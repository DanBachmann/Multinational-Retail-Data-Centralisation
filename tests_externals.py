import unittest
import pandas as pd

from database_utils import DatabaseConnector
class TestDatabaseUtils(unittest.TestCase):
    def test_list_db_tables_contain_expected(self):
        db_connector = DatabaseConnector()
        db_creds = db_connector.read_db_creds()
        db_engine = db_connector.init_db_engine(db_creds)
        table_names = db_connector.list_db_tables(db_engine)
        self.assertIn("legacy_users", table_names)
        self.assertIn("orders_table", table_names)

    def test_upload_data(self):
        db_connector = DatabaseConnector()
        data_frame = pd.DataFrame({'a':[1,2,3],'b':[4,5,6]})
        db_connector.upload_to_db(data_frame, 'test')


from data_extraction import DataExtractor
class TestDataExtractor(unittest.TestCase):
    def test_read_rds_table(self):
        db_connector = DatabaseConnector()
        db_creds = db_connector.read_db_creds()
        db_engine = db_connector.init_db_engine(db_creds)
        table_names = db_connector.list_db_tables(db_engine)
        table_name_to_use = table_names[0]
        for table_name in table_names:
            if 'users' in table_name:
                table_name_to_use = table_name
                break;        
        data_extractor = DataExtractor()
        data_frame = data_extractor.read_rds_table(db_connector, table_name_to_use)
        self.assertTrue(type(data_frame),type(pd.DataFrame()))
    
    def test_retrieve_pdf_data(self):
        data_extractor = DataExtractor()
        api_config = data_extractor.read_api_creds()
        url = api_config['card_data_url']
        pdf_data_frame = data_extractor.retrieve_pdf_data(url)
        self.assertTrue(type(pdf_data_frame),type(pd.DataFrame()))

    def test_list_number_of_stores(self):
        data_extractor = DataExtractor()
        api_config = data_extractor.read_api_creds()
        api_header_dict = {'x-api-key': api_config['stores_api_key']}
        url = api_config['number_stores_url']
        number_of_stores  = data_extractor.list_number_of_stores(api_header_dict, url)
        self.assertGreaterEqual(number_of_stores,0)

    def test_retrieve_store_details(self):
        data_extractor = DataExtractor()
        api_config = data_extractor.read_api_creds()
        api_header_dict = {'x-api-key': api_config['stores_api_key']}
        template = api_config['store_data_template']
        store_details = data_extractor.retrieve_stores_data(api_header_dict, template, 3)
        self.assertTrue(type(store_details),type(pd.DataFrame()))

    def test_extract_from_s3(self):
        data_extractor = DataExtractor()
        api_config = data_extractor.read_api_creds()
        products_csv_uri = api_config['products_csv_uri']
        products_data_frame = data_extractor.extract_from_s3(products_csv_uri)
        self.assertTrue(type(products_data_frame),type(pd.DataFrame()))

    def test_extract_from_json_http(self):
        data_extractor = DataExtractor()
        api_config = data_extractor.read_api_creds()
        data_frame = data_extractor.extract_from_json(api_config['date_details_url'])
        self.assertTrue(type(data_frame),type(pd.DataFrame()))


if __name__ == '__main__':
    unittest.main()
