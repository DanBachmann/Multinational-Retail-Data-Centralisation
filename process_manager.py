import logging
import threading
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import sqlalchemy.types as types

class ProcessManager:
    # initialise stateless worker classes
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()
    # read external endpoint configurations
    api_config =  data_extractor.read_api_creds()

    # to be filled in later during __init__ or thread creation
    write_raw_data = False
    thread_function_list = []
    valid_arguments_list = []

    def __init__(self):
        self.thread_function_list = [self.process_users, self.process_cards, self.process_stores, self.process_products, self.process_orders, self.process_times,
            self.do_nothing]
        self.valid_arguments_list = ['checks_extensive', 'checks', 'write_raw']
        for thread_function in self.thread_function_list:
            self.valid_arguments_list.append(thread_function.__name__)

    def prerequisite_checks_ok(self, extensive):
        logging.info("PREREQUISITE CHECK: database configurations load")
        db_creds = self.db_connector.read_db_creds()
        db_engine = self.db_connector.init_db_engine(db_creds)
        if not db_engine:
            logging.error(f"PREREQUISITE CHECK FAILED: source database configuration failed")
            return False
        if not self.db_connector.init_db_engine(db_creds, 'LOCAL_'):
            logging.error(f"PREREQUISITE CHECK FAILED: target database configuration failed")
            return False

        logging.info("PREREQUISITE CHECK: source user database table")
        user_table = 'legacy_users'
        table_names = self.db_connector.list_db_tables(db_engine)
        if user_table not in table_names:
            logging.error(f"PREREQUISITE CHECK FAILED: table {user_table} not found in database")
            return False

        logging.info("PREREQUISITE CHECK: card PDF URL & library installed")
        card_data_url = self.api_config['card_data_url']
        data_frame =  self.data_extractor.retrieve_pdf_data(card_data_url, 1)
        if data_frame.empty:
            logging.error("PREREQUISITE CHECK FAILED: card PDF empty")
            return False

        logging.info("PREREQUISITE CHECK: write access to target database")
        self.db_connector.upload_to_db(data_frame, 'test')

        if extensive:        
            logging.info("PREREQUISITE CHECK: API endpoints")
            api_header_dict = {'x-api-key': self.api_config['stores_api_key']}
            number_stores_url = self.api_config['number_stores_url']
            store_data_template = self.api_config['store_data_template']
            number_of_stores =  self.data_extractor.list_number_of_stores(api_header_dict, number_stores_url)
            if number_of_stores <1:
                logging.error("PREREQUISITE CHECK FAILED: no stores in list_number_of_stores API")
                return False
            data_frame =  self.data_extractor.retrieve_stores_data(api_header_dict, store_data_template, 1)
            if data_frame.empty:
                logging.error("PREREQUISITE CHECK FAILED: store not found in retrieve_stores_data API")
                return False
            
            logging.info("PREREQUISITE CHECK: product data from S3")
            products_csv_uri = self.api_config['products_csv_uri']
            data_frame =  self.data_extractor.extract_from_s3(products_csv_uri)
            if data_frame.empty:
                logging.error(f"PREREQUISITE CHECK FAILED: producs not found in {products_csv_uri}")
                return False
            
            logging.info("PREREQUISITE CHECK: time data from HTTPS")
            date_details_url = self.api_config['date_details_url']
            data_frame =  self.data_extractor.extract_from_json(date_details_url)
            if data_frame.empty:
                logging.error(f"PREREQUISITE CHECK FAILED: time data not found in {date_details_url}")
                return False

        logging.info("PREREQUISITE CHECKS: DONE")
        return True

    def initialise_threads(self, argv):
        write_raw_data = 'write_raw' in  argv

        # do some optinal checks to be sure we are connected to the internet and critial components can execute
        extensive_checks = 'checks_extensive' in  argv
        if extensive_checks or 'checks' in  argv:
            if not self.prerequisite_checks_ok(extensive_checks):
                logging.error("PREREQUISITE CHECK FAILED: FAILED. Exiting.")
                exit()

        # since these processes are heavily io bound with different sources we can run them in parallel for a performance increase
        thread_list = []
        # if we have specified processes on the command line, then run those; otherwise, run them all
        for thread_function in self.thread_function_list:
            if thread_function.__name__ in  argv:
                if thread_list == []:
                    logging.info("running specified processes only")
                thread = threading.Thread(target=thread_function, args=())
                thread_list.append(thread)
        if thread_list == []:
            for thread_function in self.thread_function_list:
                thread = threading.Thread(target=thread_function, args=())
                thread_list.append(thread)
        return thread_list
        
    def __upload_to_db_raw(self, data_frame, table_name):
        if self.write_raw_data:
            return self.__upload_to_db(data_frame, table_name+'_raw')
        return data_frame.shape[0]
    def __upload_to_db(self, data_frame, table_name, start_size, dtypes=None):
        if start_size is not None:
            reduction_percent = 100-100*data_frame.shape[0]/start_size
            if reduction_percent > 10:
                logging.warn(f"{table_name}: {start_size} rows -> {data_frame.shape[0]} rows = {round( reduction_percent,1)}% SIGNIFICANT  reduction")
            elif reduction_percent > 0:
                logging.info(f"{table_name}: {start_size} rows -> {data_frame.shape[0]} rows = {round( reduction_percent,1)}%  reduction")
        logging.info("saving to database in "+table_name)
        self.db_connector.upload_to_db(data_frame, table_name, dtypes)
        return data_frame.shape[0]

    def process_users(self):
        logging.info("USERS: reading data from AWS database")
        source_table = 'legacy_users'
        data_frame =  self.data_extractor.read_rds_table(self.db_connector, source_table)
        logging.info("USERS: cleaning data")
        table_name = "dim_users"
        start_size = self.__upload_to_db_raw(data_frame, table_name)
        data_frame =  self.data_cleaning.clean_user_data(data_frame)

        # convert types as specified in milestone 3
        # | first_name     | TEXT               | VARCHAR(255)       |
        # | last_name      | TEXT               | VARCHAR(255)       |
        # | date_of_birth  | TEXT               | DATE               |
        # | country_code   | TEXT               | VARCHAR(?)         |
        # | user_uuid      | TEXT               | UUID               |
        # | join_date      | TEXT               | DATE               |
        country_code_max_len = data_frame.country_code.map(lambda x: len(x)).max()
        dtypes={'first_name': types.VARCHAR(255), 'last_name': types.VARCHAR(255), 'country_code': types.VARCHAR(country_code_max_len),
            'date_of_birth': types.DATE, 'join_date': types.DATE,
            'user_uuid': types.UUID}
        self.__upload_to_db(data_frame, table_name, start_size, dtypes)
        logging.info("USERS: DONE")

    def process_orders(self):
        logging.info("ORDERS: reading data from AWS database")
        source_table = 'orders_table'
        data_frame =  self.data_extractor.read_rds_table(self.db_connector, source_table)
        logging.info("ORDERS: cleaning data")
        table_name = 'orders_table'
        start_size = self.__upload_to_db_raw(data_frame, table_name)
        data_frame =  self.data_cleaning.clean_orders_data(data_frame)

        # convert types as specified in milestone 3
        # | date_uuid        | TEXT               | UUID               |
        # | user_uuid        | TEXT               | UUID               |
        # | card_number      | TEXT               | VARCHAR(?)         |
        # | store_code       | TEXT               | VARCHAR(?)         |
        # | product_code     | TEXT               | VARCHAR(?)         |
        # | product_quantity | BIGINT             | SMALLINT           |
        store_code_max_len = data_frame.store_code.map(lambda x: len(x)).max()
        card_number_max_len = data_frame.card_number.map(lambda x: len(str(x))).max()
        product_code_max_len = data_frame.product_code.map(lambda x: len(str(x))).max()
        dtypes={'store_code': types.VARCHAR(store_code_max_len), 'card_number': types.VARCHAR(card_number_max_len), 'product_code': types.VARCHAR(product_code_max_len),
            'product_quantity': types.SMALLINT, 'date_uuid': types.UUID, 'user_uuid': types.UUID}
        self.__upload_to_db(data_frame, table_name, start_size, dtypes)
        logging.info("ORDERS: DONE")

    def process_cards(self):
        logging.info("CARDS: reading data from HTTPS PDF")
        card_data_url = self.api_config['card_data_url']
        data_frame =  self.data_extractor.retrieve_pdf_data(card_data_url)
        logging.info("CARDS: cleaning data")
        table_name = 'dim_card_details'
        start_size = self.__upload_to_db_raw(data_frame, table_name)
        data_frame =  self.data_cleaning.clean_card_data(data_frame)

        # convert types as specified in milestone 3
        # | card_number            | TEXT              | VARCHAR(?)         |
        # | expiry_date            | TEXT              | VARCHAR(?)         |
        # | date_payment_confirmed | TEXT              | DATE               |
        card_number_max_len = data_frame.card_number.map(lambda x: len(str(x))).max()
        expiry_date_max_len = data_frame.expiry_date.map(lambda x: len(str(x))).max()
        dtypes={'card_number': types.VARCHAR(card_number_max_len), 'expiry_date': types.VARCHAR(expiry_date_max_len), 'date_payment_confirmed': types.DATE}
        self.__upload_to_db(data_frame, table_name, start_size, dtypes)
        logging.info("CARDS: DONE")

    def process_stores(self):
        logging.info("STORES: reading data from API")
        api_header_dict = {'x-api-key': self.api_config['stores_api_key']}
        number_stores_url = self.api_config['number_stores_url']
        store_data_template = self.api_config['store_data_template']
        number_of_stores =  self.data_extractor.list_number_of_stores(api_header_dict, number_stores_url)
        data_frame =  self.data_extractor.retrieve_stores_data(api_header_dict, store_data_template, number_of_stores)
        logging.info("STORES: cleaning data")
        table_name = 'dim_store_details'
        start_size = self.__upload_to_db_raw(data_frame, table_name)
        data_frame =  self.data_cleaning.clean_store_data(data_frame)

        # convert types as specified in milestone 3
        # | longitude           | TEXT              | FLOAT                  |
        # | locality            | TEXT              | VARCHAR(255)           |
        # | store_code          | TEXT              | VARCHAR(?)             |
        # | staff_numbers       | TEXT              | SMALLINT               |
        # | opening_date        | TEXT              | DATE                   |
        # | store_type          | TEXT              | VARCHAR(255) NULLABLE  |
        # | latitude            | TEXT              | FLOAT                  |
        # | country_code        | TEXT              | VARCHAR(?)             |
        # | continent           | TEXT              | VARCHAR(255)           |
        store_code_max_len = data_frame.store_code.map(lambda x: len(x)).max()
        country_code_max_len = data_frame.country_code.map(lambda x: len(x)).max()
        dtypes={'store_code': types.VARCHAR(store_code_max_len),
            'locality': types.VARCHAR(255), 'country_code': types.VARCHAR(country_code_max_len), 'continent': types.VARCHAR(255),
            'staff_numbers': types.SMALLINT, 'opening_date': types.DATE,
            'longitude': types.FLOAT, 'latitude': types.FLOAT
            }
        self.__upload_to_db(data_frame, table_name, start_size, dtypes)
        logging.info("STORES: DONE")

    def process_products(self):
        logging.info("PRODUCTS: reading data from S3")
        products_csv_uri = self.api_config['products_csv_uri']
        data_frame =  self.data_extractor.extract_from_s3(products_csv_uri)
        logging.info("PRODUCTS: cleaning data")
        table_name = 'dim_products'
        start_size = self.__upload_to_db_raw(data_frame, table_name)
        data_frame =  self.data_cleaning.clean_products_data(data_frame)

        # convert types as specified in milestone 3
        # | product_price   | TEXT               | FLOAT              |
        # | weight          | TEXT               | FLOAT              |
        # | EAN             | TEXT               | VARCHAR(?)         |
        # | product_code    | TEXT               | VARCHAR(?)         |
        # | date_added      | TEXT               | DATE               |
        # | uuid            | TEXT               | UUID               |
        # | still_available | TEXT               | BOOL               |
        # | weight_class    | TEXT               | VARCHAR(?)         |
        EAN_max_len = data_frame.EAN.map(lambda x: len(str(x))).max()
        product_code_max_len = data_frame.product_code.map(lambda x: len(str(x))).max()
        weight_class_max_len = data_frame.weight_class.map(lambda x: len(x)).max()
        dtypes={'EAN': types.VARCHAR(EAN_max_len), 'product_code': types.VARCHAR(product_code_max_len), 'weight_class': types.VARCHAR(weight_class_max_len),
            'continent': types.VARCHAR(255),
            'uuid': types.UUID, 'date_added': types.DATE,
            'product_price': types.FLOAT, 'weight': types.FLOAT,
            'currency': types.VARCHAR(3)
            }
        self.__upload_to_db(data_frame, table_name, start_size, dtypes)
        logging.info("PRODUCTS: DONE")

    def process_times(self):
        logging.info("TIME: reading data from HTTPS")
        date_details_url = self.api_config['date_details_url']
        data_frame =  self.data_extractor.extract_from_json(date_details_url)
        logging.info("TIME: cleaning data")
        table_name = 'dim_date_times'
        start_size = self.__upload_to_db_raw(data_frame, table_name)
        data_frame =  self.data_cleaning.clean_time_data(data_frame)

        # | month           | TEXT              | VARCHAR(?) - Already handled by making a standard datetime stamp. Will leave as is unless the project requires it different later.
        # | year            | TEXT              | VARCHAR(?) - Already handled by making a standard datetime stamp. Will leave as is unless the project requires it different later.
        # | day             | TEXT              | VARCHAR(?) - Already handled by making a standard datetime stamp. Will leave as is unless the project requires it different later.
        # | time_period     | TEXT              | VARCHAR(?) - Already handled by making a standard datetime stamp. Will leave as is unless the project requires it different later.
        # | date_uuid       | TEXT              | UUID               |
        dtypes={'date_uuid': types.UUID}
        self.__upload_to_db(data_frame, table_name, start_size, dtypes)
        logging.info("TIME: DONE")

    # dummy function - when if specified alone, no threads will run. Useful for just doing pre-requisite checks.
    def do_nothing(self):
        return
