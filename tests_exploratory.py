import unittest
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class TestDataExtractor(unittest.TestCase):
    def test_read_rds_table(self):
        db_connector = DatabaseConnector()
        db_creds = db_connector.read_db_creds()
        db_engine = db_connector.init_db_engine(db_creds)
        table_names = db_connector.list_db_tables(db_engine)
        print(table_names)
        self.assertGreater(len(table_names),1)
        # data_extractor = DataExtractor()
        # data_frame = data_extractor.read_rds_table(db_connector, table_name_to_use)
        # self.assertTrue(type(data_frame),type(pd.DataFrame()))

if __name__ == '__main__':
    unittest.main()
