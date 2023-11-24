import unittest
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class TestDatabaseUtils(unittest.TestCase):
    def test_read_db_creds(self):
        db_connector = DatabaseConnector()
        db_creds = db_connector.read_db_creds()
        self.assertEqual(type(db_creds), type({}))
        self.assertTrue(db_creds['RDS_HOST'])
        self.assertTrue(db_creds['RDS_USER'])
        self.assertTrue(db_creds['RDS_PASSWORD'])
        self.assertTrue(db_creds['RDS_PORT'])
        self.assertTrue(db_creds['RDS_DATABASE'])
        self.assertTrue(db_creds['RDS_DATABASE_TYPE'])
        self.assertTrue(db_creds['LOCAL_HOST'])
        self.assertTrue(db_creds['LOCAL_USER'])
        self.assertTrue(db_creds['LOCAL_PASSWORD'])
        self.assertTrue(db_creds['LOCAL_PORT'])
        self.assertTrue(db_creds['LOCAL_DATABASE'])
        self.assertTrue(db_creds['LOCAL_DATABASE_TYPE'])
    
    def test_read_api_creds(self):
        db_extractor = DataExtractor()
        api_creds = db_extractor.read_api_creds()
        self.assertTrue(api_creds['stores_api_key'])
        self.assertTrue(api_creds['number_stores_url'])
        self.assertTrue(api_creds['store_data_template'])
        self.assertTrue(api_creds['card_data_url'])
        self.assertTrue(api_creds['products_csv_uri'])
        self.assertTrue(api_creds['date_details_url'])

    def test_init_db_engine(self):
        db_connector = DatabaseConnector()
        db_creds = db_connector.read_db_creds()
        db_engine = db_connector.init_db_engine(db_creds)
        self.assertIsNotNone(db_engine)

if __name__ == '__main__':
    unittest.main()
