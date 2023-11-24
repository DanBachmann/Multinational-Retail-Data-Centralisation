import yaml
import sqlalchemy

class DatabaseConnector:
    # read the credentials yaml file and return a dictionary of the credentials.
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            config_data = yaml.safe_load(file)
        return config_data

    # read the credentials from the return of db_creds and initialise and return an sqlalchemy database engine
    def init_db_engine(self, config_data, prefix='RDS_'):
        DBAPI = 'psycopg2'
        engine_url = f"{config_data[prefix + 'DATABASE_TYPE']}+{DBAPI}://{config_data[prefix + 'USER']}:{config_data[prefix + 'PASSWORD']}@{config_data[prefix + 'HOST']}:{config_data[prefix + 'PORT']}/{config_data[prefix + 'DATABASE']}"
        engine = sqlalchemy.create_engine(engine_url)
        return engine

    # list the tables in the database for exploratory work
    def list_db_tables(self, engine):
        inspector = sqlalchemy.inspect(engine)
        return inspector.get_table_names()

    # upload data to database table. NOTE: table & data will be replaced
    def upload_to_db(self, source_data_frame, target_table_name):
        db_creds = self.read_db_creds()
        target_engine = self.init_db_engine(db_creds, 'LOCAL_')
        with target_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as con:
            source_data_frame.to_sql(target_table_name, target_engine, if_exists='replace', index=False)
