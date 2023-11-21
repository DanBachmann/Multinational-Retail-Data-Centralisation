import logging
import threading
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def prerequisite_checks_ok(db_connector, data_extractor, extensive):
    logging.info("PREREQUISITE CHECK: database configurations load")
    db_creds = db_connector.read_db_creds()
    db_engine = db_connector.init_db_engine(db_creds)
    if not db_engine:
        logging.warn(f"PREREQUISITE CHECK FAILED: source database configuration failed")
        return False
    if not db_connector.init_db_engine(db_creds, 'LOCAL_'):
        logging.warn(f"PREREQUISITE CHECK FAILED: target database configuration failed")
        return False

    logging.info("PREREQUISITE CHECK: source user database table")
    user_table = 'legacy_users'
    table_names = db_connector.list_db_tables(db_engine)
    if user_table not in table_names:
        logging.warn(f"PREREQUISITE CHECK FAILED: table {user_table} not found in database")
        return False

    logging.info("PREREQUISITE CHECK: card PDF")
    data_frame = data_extractor.retrieve_pdf_data(card_data_url, 1)
    if data_frame.empty:
        logging.warn("PREREQUISITE CHECK FAILED: card PDF empty")
        return False

    logging.info("PREREQUISITE CHECK: write access to target database")
    db_connector.upload_to_db(data_frame, 'test')

    if extensive:        
        logging.info("PREREQUISITE CHECK: API endpoints")
        number_of_stores = data_extractor.list_number_of_stores(api_header_dict, number_stores_url)
        if number_of_stores <1:
            logging.warn("PREREQUISITE CHECK FAILED: no stores in list_number_of_stores API")
            return False
        store_id = 1
        data_frame = data_extractor.retrieve_stores_data(api_header_dict, store_data_template, store_id, False)
        if data_frame.empty:
            logging.warn("PREREQUISITE CHECK FAILED: store not found in retrieve_stores_data API")
            return False
        
        logging.info("PREREQUISITE CHECK: product data from S3")
        data_frame = data_extractor.extract_from_s3(products_csv_uri)
        if data_frame.empty:
            logging.warn(f"PREREQUISITE CHECK FAILED: producs not found in {products_csv_uri}")
            return False
    
    logging.info("PREREQUISITE CHECKS: DONE")
    return True

def process_users(db_connector, data_extractor, data_cleaning):
    # TODO: error handling
    logging.info("USERS: reading data from AWS database")
    source_table = 'legacy_users'
    data_frame = data_extractor.read_rds_table(db_connector, source_table)
    logging.info("USERS: cleaning data")
    clean_data_frame = data_cleaning.clean_user_data(data_frame)
    logging.info("USERS: saving to database")
    db_connector.upload_to_db(clean_data_frame, 'dim_users')
    logging.info("USERS: DONE")

def process_orders(db_connector, data_extractor, data_cleaning):
    logging.info("ORDERS: reading data from AWS database")
    source_table = 'orders_table'
    data_frame = data_extractor.read_rds_table(db_connector, source_table)
    logging.info("ORDERS: cleaning data")
    clean_data_frame = data_cleaning.clean_orders_data(data_frame)
    logging.info("ORDERS: saving to database")
    db_connector.upload_to_db(clean_data_frame, 'orders_table')
    logging.info("ORDERS: DONE")

def process_cards(db_connector, data_extractor, data_cleaning):
    logging.info("CARDS: reading data from HTTPS PDF")
    data_frame = data_extractor.retrieve_pdf_data(card_data_url)
    logging.info("CARDS: cleaning data")
    clean_data_frame = data_cleaning.clean_card_data(data_frame)
    logging.info("CARDS: saving to database")
    db_connector.upload_to_db(clean_data_frame, 'dim_card_details')
    logging.info("CARDS: DONE")

def process_stores(db_connector, data_extractor, data_cleaning):
    logging.info("STORES: reading data from API")
    number_of_stores = data_extractor.list_number_of_stores(api_header_dict, number_stores_url)
    data_frame = data_extractor.retrieve_stores_data(api_header_dict, store_data_template, number_of_stores, False)
    logging.info("STORES: cleaning data")
    clean_data_frame = data_cleaning.called_clean_store_data(data_frame)
    logging.info("STORES: saving to database")
    db_connector.upload_to_db(clean_data_frame, 'dim_store_details')
    logging.info("STORES: DONE")

def process_products(db_connector, data_extractor, data_cleaning):
    logging.info("PRODUCTS: reading data from S3")
    products_data_frame = data_extractor.extract_from_s3(products_csv_uri)
    logging.info("PRODUCTS: cleaning data")
    clean_data_frame = data_cleaning.convert_product_weights(products_data_frame)
    clean_data_frame = data_cleaning.clean_products_data(clean_data_frame)
    logging.info("PRODUCTS: saving to database")
    db_connector.upload_to_db(clean_data_frame, 'dim_products')
    logging.info("PRODUCTS: DONE")

def process_time_data(db_connector, data_extractor, data_cleaning):
    logging.info("TIME: reading data from HTTPS")
    data_frame = data_extractor.extract_from_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    logging.info("TIME: cleaning data")
    clean_data_frame = data_cleaning.clean_time_data(data_frame)
    logging.info("TIME: saving to database")
    db_connector.upload_to_db(clean_data_frame, 'dim_date_times')
    logging.info("TIME: DONE")


logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S")
logging.info("Multinational Retail Data Centralisation project starting")

db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

api_config = data_extractor.read_api_creds()
api_header_dict = {'x-api-key': api_config['x-api-key']}
number_stores_url = api_config['number_stores_url']
store_data_template = api_config['store_data_template']
card_data_url = api_config['card_data_url']
products_csv_uri = api_config['products_csv_uri']

if not prerequisite_checks_ok(db_connector, data_extractor, False):
    logging.error("PREREQUISITE CHECK FAILED: FAILED. Exiting.")
    exit()

# since these processes are heavily io bound with different sources we can run them in parallel for a performance increase of about 25%
process_users_thread = threading.Thread(target=process_users, args=(db_connector, data_extractor, data_cleaning))
process_cards_thread = threading.Thread(target=process_cards, args=(db_connector, data_extractor, data_cleaning))
process_stores_thread = threading.Thread(target=process_stores, args=(db_connector, data_extractor, data_cleaning))
process_products_thread = threading.Thread(target=process_products, args=(db_connector, data_extractor, data_cleaning))
process_orders_thread = threading.Thread(target=process_orders, args=(db_connector, data_extractor, data_cleaning))
process_time_thread = threading.Thread(target=process_time_data, args=(db_connector, data_extractor, data_cleaning))

process_stores_thread.start()
process_users_thread.start()
process_cards_thread.start()
process_products_thread.start()
process_orders_thread.start()
process_time_thread.start()

# wait for all threads to finish
process_users_thread.join()
process_cards_thread.join()
process_products_thread.join()
process_orders_thread.join()
process_time_thread.join()
logging.info("STORES: still processing - can take awhile to load the data")
process_stores_thread.join()

logging.info("all done")
