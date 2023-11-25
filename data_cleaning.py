import pandas as pd
import numpy as np
import re

class DataCleaning:
    # clean the user data - handle NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
    def __handle_nulls_empties_and_duplicates(self, data_frame):
        for column in data_frame.columns:
                data_frame[column] = data_frame[column].apply(self.__replace_null_strings)
        data_frame.drop_duplicates(inplace=True)
        # remove completely empty columns & rows in the dataframe
        data_frame.dropna(how="all", axis=1, inplace=True)
        data_frame.dropna(how="all", axis=0, inplace=True)
        return data_frame
    
    def __replace_null_strings(self, cell_value):
        if type(cell_value) == str and cell_value.lower() in ['null','none', 'nan']:
            cell_value = None
        return cell_value
    
    def __remove_unwanted_characters(self, cell_value):
        if type(cell_value) == str:
            cell_value = cell_value.replace('?','')
        return cell_value
    
    def __remove_nonnumeric_characters(self, cell_value):
        if type(cell_value) == str:
            cell_value = re.sub("[^0-9.]", "",cell_value)
            if cell_value == '':
                cell_value = None
        return cell_value

    def __reformat_phone_data(self, data_frame, column_name):
        regex_expression = r'^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        data_frame.loc[~data_frame[column_name].str.match(regex_expression), column_name] = None # For every row where the column_name column does not match our regular expression, replace the value with None/null
        data_frame[column_name] = data_frame[column_name].replace({r'\+44(0)': '0',r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)

    # Convert a weights to a decimal value represented in kg
    def __convert_product_weight(self, raw_weight):
        '''
        Convert a weight to a decimal value represented in kg. If no units specified, presumes the weight is already in kg.
        For ml, a 1:1 ratio of ml to g is used as a rough estimate for the rows containing ml.
            Parameters:
                    raw_weight (object): The weight cell value.
            Returns:
                    weight_in_kg (float): The weight in kg or None if the input value could not be converted.
        '''
        if type(raw_weight) != str:
            try:
                return abs(float(raw_weight))
            except ValueError:
                return None
        # clean up characters that won't impact the value
        new_weight = raw_weight.replace(' ', '')
        if new_weight.endswith('.'):
            new_weight = new_weight[:len(new_weight)-1]
        # determine if we have multiple weights to handle
        divisor = 1
        new_weight_split = new_weight.split('x')
        if len(new_weight_split) > 1:
            new_weight = new_weight_split[1]
            try:
                divisor = 1/float(new_weight_split[0])
            except ValueError:
                return None
        # convert known units to kg and remove the unit designation
        if new_weight.endswith('kg'):
            new_weight = new_weight.replace('kg', '')
        elif new_weight.endswith('g') or new_weight.endswith('ml'):
            new_weight = new_weight.replace('g', '').replace('ml', '')
            divisor = 1000
        # try the value as a float and consider mathmatic adjustments. if it fails, it is bad data
        try:
            weight_in_kg = abs(float(new_weight)/divisor)
            return weight_in_kg
        except ValueError:
            return None

    # Convert all weights in a dataframe column to a decimal value represented in kg
    def __convert_product_weights(self, products_data_frame):
        '''
        Modifiies dataframe column weight values to a decimal value represented in kg. If no units specified, presumes the weight is already in kg.
        For ml, a 1:1 ratio of ml to g is used as a rough estimate for the rows containing ml.
            Parameters:
                    products_data_frame (dataframe): Dataframe with 'weight' as the column to manipulate.
            Returns:
                    data_frame (Pandas Dataframe): The modified dataframe.
        '''
        products_data_frame['weight'] = products_data_frame['weight'].apply(self.__convert_product_weight)
        products_data_frame.weight = products_data_frame.weight.astype('float')
        return products_data_frame
    
    def __assign_weight_class(self, weight):
        if weight < 2:
            return 'Light'
        if weight < 40:
            return 'Mid_Sized'
        if weight < 140:
            return 'Heavy'
        return 'Truck_Required'

    def clean_user_data(self, data_frame):
        '''
        Cleans legacy user data and sets column types as appropriate.
                Removes columns and rows with all null data and removes duplicates
                Standardises phone number formatting.
                Standardises GB country_code.
                Removes invalid email_address rows.
            Parameters:
                    data_frame (dataframe): Dataframe with the legacy user data.
            Returns:
                    data_frame (Pandas Dataframe): The modified dataframe.
        '''
        # we have the uuid as a unique key, so we can drop the index column
        data_frame.drop(['index'], axis=1, inplace=True)
        # check date errors & set as date time type
        data_frame['date_of_birth'] = pd.to_datetime(data_frame['date_of_birth'], format='mixed', errors='coerce')
        data_frame['join_date'] = pd.to_datetime(data_frame['join_date'], format='mixed', errors='coerce')
        # check NULL values and remove duplicates
        data_frame = self.__handle_nulls_empties_and_duplicates(data_frame)
        # format phone numbers
        self.__reformat_phone_data(data_frame, 'phone_number')
        # fix country_code
        mask_country = data_frame['country'] == 'United Kingdom'
        data_frame.loc[mask_country, 'country_code'] = 'GB'
        # email_addresses - the data without a simple @ in the email address has the entire row as invalid in this table, so remove those rows
        mask_email_address = data_frame['email_address'].str.contains('@')
        data_frame = data_frame.loc[mask_email_address]
        return data_frame

    # remove any erroneous values, NULL values or errors with formatting.
    def clean_card_data(self, data_frame):
        '''
        Cleans legacy card data and sets column types as appropriate.
                Removes columns and rows with all null data and removes duplicates
                Standardises card_number number formatting.
                Standardises date_payment_confirmed.
            Parameters:
                    data_frame (Pandas dataframe): Dataframe with the legacy card data.
            Returns:
                    data_frame (Pandas Dataframe): The modified dataframe.
        '''
        data_frame = self.__handle_nulls_empties_and_duplicates(data_frame)
        # clean card_number to int64. if there is invalid data, there is no use for the entry without the card_number key, so remove it as these are relatively few entries anyway
        data_frame['card_number'] = data_frame['card_number'].apply(self.__remove_unwanted_characters)
        data_frame['card_number'] = pd.to_numeric(data_frame['card_number'], errors='coerce')
        data_frame.dropna(inplace=True)
        data_frame['card_number'] = data_frame['card_number'].astype('int64', errors='ignore')
        # set date_payment_confirmed as date type
        data_frame['date_payment_confirmed'] = data_frame['date_payment_confirmed'].apply(self.__remove_unwanted_characters)
        data_frame['date_payment_confirmed'] = pd.to_datetime(data_frame['date_payment_confirmed'], errors='coerce')
        return data_frame

    def clean_store_data(self, data_frame):
        '''
        Cleans legacy store data and sets column types as appropriate.
                Removes columns and rows with all null data and removes duplicates.
                Standardises longititude & latitude number formatting.
                Standardises staff_numbers number type.
                Standardises opening_date datetime type.
                Reorders column in a logical way by grouping related columns together.
            Parameters:
                    data_frame (Pandas dataframe): Dataframe with the legacy card data.
            Returns:
                    data_frame (Pandas Dataframe): The modified dataframe.
        '''
        # we have the store_key as a unique key, so we can drop the index column
        data_frame.drop(['index'], axis=1, inplace=True)
        # remove lat column which is empty and replaced with the latitude column
        data_frame.drop(['lat'], axis=1, inplace=True)
        data_frame = self.__handle_nulls_empties_and_duplicates(data_frame)
        # we know that invalid store_type, continent or country_code are all on same rows which are all invalid data, so remove these first
        # we didn't filter on country_code or continent because they have more values or values that are likely to expand in the future
        data_frame = data_frame[data_frame['store_type'].isin(["Mall Kiosk","Super Store","Local","Web Portal","Outlet"])]
        # fix some invalid data in continent field
        pd.options.mode.chained_assignment = None  # default='warn'
        mask_continent = data_frame['continent'] == 'eeEurope'
        data_frame.loc[mask_continent, 'continent'] = 'Europe'
        mask_continent = data_frame['continent'] == 'eeAmerica'
        data_frame.loc[mask_continent, 'continent'] = 'America'
        # remove non-numerical characters from float values and set type
        data_frame['longitude'] = data_frame['longitude'].apply(self.__remove_nonnumeric_characters)
        data_frame['longitude'] = data_frame['longitude'].astype('float', errors='ignore')
        data_frame['latitude'] = data_frame['latitude'].apply(self.__remove_nonnumeric_characters)
        data_frame['latitude'] = data_frame['latitude'].astype('float', errors='ignore')
        # remove non-numerical characters from int values and set type
        data_frame['staff_numbers'] = data_frame['staff_numbers'].apply(self.__remove_nonnumeric_characters)
        data_frame['staff_numbers'] = data_frame['staff_numbers'].astype('int32', errors='raise')
        # standardise date type
        data_frame['opening_date'] = pd.to_datetime(data_frame['opening_date'], format='mixed', errors='ignore')
        pd.options.mode.chained_assignment = 'warn'  # back to default mode
        # re-order so into a more logical order of identification, attributes, location
        data_frame = data_frame[['store_code', 'store_type', 'staff_numbers', 'opening_date', 'address','locality', 'continent', 'country_code', 'longitude', 'latitude']]
        return data_frame
    
    def clean_products_data(self, data_frame):
        '''
        Cleans legacy products data and sets column types as appropriate.
                Removes columns and rows with all null data and removes duplicates.
                Splits currency into a new column from product_price.
                Standardises date_added datetime type.
                Standardises removed flag to a boolean.
            Parameters:
                    data_frame (Pandas dataframe): Dataframe with the legacy products data.
            Returns:
                    data_frame (Pandas Dataframe): The modified dataframe.
        '''
        # the 'Unnamed: 0' column looks like the index. we have the uuid, so we can drop it
        data_frame.drop(['Unnamed: 0'], axis=1, inplace=True)
        data_frame = self.__convert_product_weights(data_frame)
        data_frame['currency'] = data_frame['product_price'].apply(lambda x: x[:1] if type(x)==str else '£')
        pd.options.mode.chained_assignment = None  # default='warn'
        # rows with no currency symbol are bogus based on our data review so remove them
        regex_expression = r'^[£€\$]'
        data_frame = data_frame.loc[data_frame['currency'].str.match(regex_expression)]
        data_frame['product_price'] = data_frame['product_price'].apply(self.__remove_nonnumeric_characters)
        data_frame.product_price = data_frame.product_price.astype('float')
        data_frame['date_added'] = pd.to_datetime(data_frame['date_added'], errors='coerce')
        data_frame = self.__handle_nulls_empties_and_duplicates(data_frame)
        data_frame['still_available'] = data_frame['removed'].apply(lambda x: False if x is not None and type(x) == str and x.lower() == 'removed' else True)
        data_frame['still_available'] = data_frame['still_available'].astype('bool')
        data_frame.drop(['removed'], axis=1, inplace=True)
        data_frame['weight_class'] = data_frame['weight'].apply(self.__assign_weight_class)
        pd.options.mode.chained_assignment = 'warn'
        return data_frame

    def clean_orders_data(self, data_frame):
        '''
        Cleans legacy orders data and sets column types as appropriate.
                Removes columns and rows with all null data including first_name, last_name & '1' and removes duplicates.
            Parameters:
                    data_frame (Pandas dataframe): Dataframe with the legacy products data.
            Returns:
                    data_frame (Pandas Dataframe): The modified dataframe.
        '''
        # remove columns as per specification: first_name, last_name and 1
        data_frame.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        # we can drop index since we have level_0 as a unique key
        data_frame.drop(['index'], axis=1, inplace=True)
        return self.__handle_nulls_empties_and_duplicates(data_frame)

    def clean_time_data(self, data_frame):
        '''
        Cleans time table and sets column types as appropriate.
                Removes columns and rows with all null data and removes duplicates.
                Standardises date & timestamp to a single field datetime type.
            Parameters:
                    data_frame (Pandas dataframe): Dataframe with the legacy products data.
            Returns:
                    data_frame (Pandas dataframe): The modified dataframe.
        '''
        # consolodate time value fields into one datetime column
        data_frame['date_timestamp'] = data_frame['year'] + '-' + data_frame['month'] + '-' + data_frame['day'] + ' ' + data_frame['timestamp']
        data_frame['date_timestamp'] = pd.to_datetime(data_frame['date_timestamp'], errors='coerce')
        data_frame.drop(['year', 'month', 'day', 'timestamp'], axis=1, inplace=True)
        # bad dates will be null now. there's no useful information in those rows, so drop them
        data_frame.dropna(inplace=True)
        return data_frame
