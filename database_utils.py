import yaml
import sqlalchemy


class DatabaseConnector:
    # read the credentials yaml file and return a dictionary of the credentials.
    def read_db_creds(self):
        '''
        Loads configuration for the databases from a file (db_creds.yaml).
            Parameters:
                    none.
            Returns:
                    config (dictionary): The values for establishing source(RDS) and target(local) database connections.
        '''
        with open('db_creds.yaml', 'r') as file:
            config = yaml.safe_load(file)
        return config

    # read the credentials from the return of db_creds and initialise and return an sqlalchemy database engine
    def init_db_engine(self, config: dict[str, str], prefix='RDS_'):
        '''
        Loads configuration for the databases from a file (db_creds.yaml).
            Parameters:
                    config (dictionary): database configuration.
                    prefix (str) (optional): Determines the databse configuration to use. Options are 'RDS_' or 'LOCAL_'. Defaults to 'RDS_'.
            Returns:
                    engine (sqlalchemy database engine): The values for establishing source(RDS) and target(local) database connections.
        '''
        db_api = 'psycopg2'
        engine_url = f"{config[prefix+'DATABASE_TYPE']}+{db_api}://{config[prefix+'USER']}:{config[prefix+'PASSWORD']}@{config[prefix+'HOST']}:{config[prefix+'PORT']}/{config[prefix+'DATABASE']}"
        engine = sqlalchemy.create_engine(engine_url)
        return engine

    # list the tables in the database for exploratory work
    def list_db_tables(self, engine):
        '''
        List the tables in the database for exploratory work. Useful in a Jupyter notebook.
            Parameters:
                    engine (sqlalchemy engine): Database engine configured from which the list of tables will be read.
            Returns:
                    table_names (list): A list of the table names (strings).
        '''
        inspector = sqlalchemy.inspect(engine)
        return inspector.get_table_names()

    # upload data to database table. NOTE: table & data will be replaced
    def upload_to_db(self, source_data_frame, target_table_name: str, dtypes = None, primary_key = None):
        '''
        Save the dataframe in a table (target_table_name) on the local/target database.
            Parameters:
                    source_data_frame (Pandas dataframe): Source data to write.
                    target_table_name (str): Target table to write the data into. Note existing data in the specified table will be removed/overwritten.
                    dtypes (dictionary of sqlalchemy.types) (optional): A dictionary of column names and their corresponding SQL types.
                    primary_key (str) (optional): The name of the primary key column.
            Returns:
                    none.
        '''
        db_creds = self.read_db_creds()
        target_engine = self.init_db_engine(db_creds, 'LOCAL_')
        source_data_frame.to_sql(target_table_name, target_engine, if_exists='replace', index=False, dtype = dtypes)
        with target_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as con:
            source_data_frame.to_sql(target_table_name, target_engine, if_exists='replace', index=False, dtype = dtypes)
            if primary_key is not None:
                con.execute(sqlalchemy.text(f'ALTER TABLE public.{target_table_name} ADD PRIMARY KEY ({primary_key});'))
    
    def add_foreign_key(self, target_table_name: str, target_column: str, source_table: str, source_column: str):
        db_creds = self.read_db_creds()
        target_engine = self.init_db_engine(db_creds, 'LOCAL_')
        with target_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as con:
            con.execute(sqlalchemy.text(f'ALTER TABLE public.{target_table_name} ADD CONSTRAINT fk_{source_table}_{source_column} FOREIGN KEY ({target_column}) REFERENCES {source_table} ({source_column});'))

    def drop_foreign_key(self, target_table_name: str, source_table: str, source_column: str):
        db_creds = self.read_db_creds()
        target_engine = self.init_db_engine(db_creds, 'LOCAL_')
        try:
            with target_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as con:
                con.execute(sqlalchemy.text(f'ALTER TABLE {target_table_name} DROP CONSTRAINT fk_{source_table}_{source_column};'))
        except:
            # Key may not exist, so pass.
            pass
