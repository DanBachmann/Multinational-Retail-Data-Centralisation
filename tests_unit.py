import unittest

from database_utils import DatabaseConnector
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

    def test_init_db_engine(self):
        db_connector = DatabaseConnector()
        db_creds = db_connector.read_db_creds()
        db_engine = db_connector.init_db_engine(db_creds)
        self.assertIsNotNone(db_engine)

if __name__ == '__main__':
    unittest.main()
