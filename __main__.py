import logging
import sys
import threading
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def prerequisite_checks_ok(extensive):
    logging.info("PREREQUISITE CHECK: database configurations load")
    db_creds = db_connector.read_db_creds()
    db_engine = db_connector.init_db_engine(db_creds)
    if not db_engine:
        logging.error(f"PREREQUISITE CHECK FAILED: source database configuration failed")
        return False
    if not db_connector.init_db_engine(db_creds, 'LOCAL_'):
        logging.error(f"PREREQUISITE CHECK FAILED: target database configuration failed")
        return False

    logging.info("PREREQUISITE CHECK: source user database table")
    user_table = 'legacy_users'
    table_names = db_connector.list_db_tables(db_engine)
    if user_table not in table_names:
        logging.error(f"PREREQUISITE CHECK FAILED: table {user_table} not found in database")
        return False

    logging.info("PREREQUISITE CHECK: card PDF URL & library installed")
    data_frame = data_extractor.retrieve_pdf_data(card_data_url, 1)
    if data_frame.empty:
        logging.error("PREREQUISITE CHECK FAILED: card PDF empty")
        return False

    logging.info("PREREQUISITE CHECK: write access to target database")
    db_connector.upload_to_db(data_frame, 'test')

    if extensive:        
        logging.info("PREREQUISITE CHECK: API endpoints")
        number_of_stores = data_extractor.list_number_of_stores(api_header_dict, number_stores_url)
        if number_of_stores <1:
            logging.error("PREREQUISITE CHECK FAILED: no stores in list_number_of_stores API")
            return False
        data_frame = data_extractor.retrieve_stores_data(api_header_dict, store_data_template, 1)
        if data_frame.empty:
            logging.error("PREREQUISITE CHECK FAILED: store not found in retrieve_stores_data API")
            return False
        
        logging.info("PREREQUISITE CHECK: product data from S3")
        data_frame = data_extractor.extract_from_s3(products_csv_uri)
        if data_frame.empty:
            logging.error(f"PREREQUISITE CHECK FAILED: producs not found in {products_csv_uri}")
            return False
        
        logging.info("PREREQUISITE CHECK: time data from HTTPS")
        data_frame = data_extractor.extract_from_json(date_details_url)
        if data_frame.empty:
            logging.error(f"PREREQUISITE CHECK FAILED: time data not found in {date_details_url}")
            return False

    logging.info("PREREQUISITE CHECKS: DONE")
    return True

def upload_to_db_raw(data_frame, table_name):
    if write_raw_data:
        return upload_to_db(data_frame, table_name+'_raw')
    return data_frame.shape[0]
def upload_to_db(data_frame, table_name, start_size=None):
    if start_size is not None:
        reduction = 100-100*data_frame.shape[0]/start_size
        if reduction > 10:
            logging.warn(f"{table_name}: {start_size} rows -> {data_frame.shape[0]} rows = {round(reduction,1)}% SIGNIFICANT reduction")
        elif reduction > 0:
            logging.info(f"{table_name}: {start_size} rows -> {data_frame.shape[0]} rows = {round(reduction,1)}% reduction")
    logging.info("saving to database in "+table_name)
    db_connector.upload_to_db(data_frame, table_name)
    return data_frame.shape[0]

def process_users():
    logging.info("USERS: reading data from AWS database")
    source_table = 'legacy_users'
    data_frame = data_extractor.read_rds_table(db_connector, source_table)
    logging.info("USERS: cleaning data")
    table_name = "dim_users"
    start_size = upload_to_db_raw(data_frame, table_name)
    data_frame = data_cleaning.clean_user_data(data_frame)
    upload_to_db(data_frame, table_name, start_size)
    logging.info("USERS: DONE")

def process_orders():
    logging.info("ORDERS: reading data from AWS database")
    source_table = 'orders_table'
    data_frame = data_extractor.read_rds_table(db_connector, source_table)
    logging.info("ORDERS: cleaning data")
    table_name = 'orders_table'
    start_size = upload_to_db_raw(data_frame, table_name)
    data_frame = data_cleaning.clean_orders_data(data_frame)
    upload_to_db(data_frame, table_name, start_size)
    logging.info("ORDERS: DONE")

def process_cards():
    logging.info("CARDS: reading data from HTTPS PDF")
    data_frame = data_extractor.retrieve_pdf_data(card_data_url)
    logging.info("CARDS: cleaning data")
    table_name = 'dim_card_details'
    start_size = upload_to_db_raw(data_frame, table_name)
    data_frame = data_cleaning.clean_card_data(data_frame)
    upload_to_db(data_frame, table_name, start_size)
    logging.info("CARDS: DONE")

def process_stores():
    logging.info("STORES: reading data from API")
    number_of_stores = data_extractor.list_number_of_stores(api_header_dict, number_stores_url)
    data_frame = data_extractor.retrieve_stores_data(api_header_dict, store_data_template, number_of_stores)
    logging.info("STORES: cleaning data")
    table_name = 'dim_store_details'
    start_size = upload_to_db_raw(data_frame, table_name)
    data_frame = data_cleaning.clean_store_data(data_frame)
    upload_to_db(data_frame, table_name, start_size)
    logging.info("STORES: DONE")

def process_products():
    logging.info("PRODUCTS: reading data from S3")
    data_frame = data_extractor.extract_from_s3(products_csv_uri)
    logging.info("PRODUCTS: cleaning data")
    table_name = 'dim_products'
    start_size = upload_to_db_raw(data_frame, table_name)
    data_frame = data_cleaning.clean_products_data(data_frame)
    upload_to_db(data_frame, table_name, start_size)
    logging.info("PRODUCTS: DONE")

def process_times():
    logging.info("TIME: reading data from HTTPS")
    data_frame = data_extractor.extract_from_json(date_details_url)
    logging.info("TIME: cleaning data")
    table_name = 'dim_date_times'
    start_size = upload_to_db_raw(data_frame, table_name)
    data_frame = data_cleaning.clean_time_data(data_frame)
    upload_to_db(data_frame, table_name, start_size)
    logging.info("TIME: DONE")

# dummy function - when if specified alone, no threads will run. Useful for just doing pre-requisite checks.
def do_nothing():
    return

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S")
logging.info("Multinational Retail Data Centralisation project starting")
thread_function_list = [process_users, process_cards, process_stores, process_products, process_orders, process_times, do_nothing]

# check valid arguments
valid_arguments_list = []
valid_arguments_list.append('checks_extensive')
valid_arguments_list.append('checks')
valid_arguments_list.append('write_raw')
for thread_function in thread_function_list:
    valid_arguments_list.append(thread_function.__name__)
for arg in sys.argv:
    if arg not in valid_arguments_list and arg!='.':
        logging.error(f"invalid argument {arg} specified. valid arguments are {valid_arguments_list}")
        exit()

# initialise stateless worker classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()
# read external endpoint configurations
api_config = data_extractor.read_api_creds()
api_header_dict = {'x-api-key': api_config['stores_api_key']}
number_stores_url = api_config['number_stores_url']
store_data_template = api_config['store_data_template']
card_data_url = api_config['card_data_url']
products_csv_uri = api_config['products_csv_uri']
date_details_url = api_config['date_details_url']

# do some optinal checks to be sure we are connected to the internet and critial components can execute
write_raw_data = 'write_raw' in sys.argv
extensive_checks = 'checks_extensive' in sys.argv
if extensive_checks or 'checks' in sys.argv:
    if not prerequisite_checks_ok(extensive_checks):
        logging.error("PREREQUISITE CHECK FAILED: FAILED. Exiting.")
        exit()

# since these processes are heavily io bound with different sources we can run them in parallel for a performance increase
thread_list = []
# if we have specified processes on the command line, then run those; otherwise, run them all
for thread_function in thread_function_list:
    if thread_function.__name__ in sys.argv:
        if thread_list == []:
            logging.info("running specified processes only")
        thread = threading.Thread(target=thread_function, args=())
        thread_list.append(thread)
if thread_list == []:
    for thread_function in thread_function_list:
        thread = threading.Thread(target=thread_function, args=())
        thread_list.append(thread)

for thread in thread_list:
    thread.start()

# wait for all threads to finish
for thread in thread_list:
    thread.join()

logging.info("all done")
