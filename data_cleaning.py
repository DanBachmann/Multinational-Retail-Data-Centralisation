import pandas as pd
import numpy as np
import re

class DataCleaning:
    # clean the user data - handle NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
    def handle_nulls_empties_and_duplicates(self, data_frame):
        for column in data_frame.columns:
                data_frame[column] = data_frame[column].apply(self.replace_null_strings)
        data_frame.drop_duplicates(inplace=True)
        # remove completely empty columns in a copy of the dataframe
        data_frame.dropna(how="all", axis=1, inplace=True)
        # remove mostly empty rows - often one row is the index
        data_frame.dropna(thresh=2, axis=0, inplace=True)
        return data_frame
    
    def replace_null_strings(self, cell_value):
        if type(cell_value) == str and cell_value.lower() in ['null','none', 'nan']:
            cell_value = None
        return cell_value
    
    def remove_unwanted_characters(self, cell_value):
        if type(cell_value) == str:
            cell_value = cell_value.replace('?','')
        return cell_value
    
    def remove_nonnumeric_characters(self, cell_value):
        if type(cell_value) == str:
            cell_value = re.sub("[^0-9.]", "",cell_value)
            if cell_value == '':
                cell_value = None
        return cell_value

    # def standardise_date_characters(self, cell_value):
    #     if type(cell_value) == str:
    #         cell_value = cell_value.replace('/','-').replace(' ','-')
    #     return cell_value

    # def clear_non_integer_cells(self, cell_value, clear_value=None):
    #     if clear_value is None:
    #         clear_value = cell_value
    #     if cell_value is None:
    #         return clear_value
    #     try:
    #         int_value = int(cell_value)
    #     except ValueError:
    #         return clear_value
    #     return int_value
    
    def clean_user_data(self, data_frame):
        # check date errors & set as date time type
        data_frame['date_of_birth'] = pd.to_datetime(data_frame['date_of_birth'], format='mixed', errors='coerce')
        data_frame['join_date'] = pd.to_datetime(data_frame['join_date'], format='mixed', errors='coerce')
        # check NULL values and remove duplicates
        data_frame = self.handle_nulls_empties_and_duplicates(data_frame)
        # format phone numbers
        self.reformat_phone_data(data_frame, 'phone_number')
        # fix country_code
        mask_country = data_frame['country'] == 'United Kingdom'
        data_frame.loc[mask_country, 'country_code'] = 'GB'
        # email_addresses - the data without a simple @ in the email address has the entire row as invalid in this table, so remove those rows
        mask_email_address = data_frame['email_address'].str.contains('@')
        data_frame = data_frame.loc[mask_email_address]
        return data_frame

    # remove any erroneous values, NULL values or errors with formatting.
    def clean_card_data(self, data_frame):
        data_frame = self.handle_nulls_empties_and_duplicates(data_frame)
        # clean card_number to int64. if there is invalid data, there is no use for the entry without the card_number key, so remove it as these are relatively few entries anyway
        data_frame['card_number'] = data_frame['card_number'].apply(self.remove_unwanted_characters)
        data_frame['card_number'] = pd.to_numeric(data_frame['card_number'], errors='coerce')
        data_frame.dropna(inplace=True)
        data_frame['card_number'] = data_frame['card_number'].astype('int64', errors='ignore')
        # set date_payment_confirmed as date type
        data_frame['date_payment_confirmed'] = data_frame['date_payment_confirmed'].apply(self.remove_unwanted_characters)
        data_frame['date_payment_confirmed'] = pd.to_datetime(data_frame['date_payment_confirmed'], errors='coerce')
        return data_frame

    def called_clean_store_data(self, data_frame):
        # remove lat column which is empty and replaced with the latitude column
        data_frame.drop(['lat'], axis=1, inplace=True)
        data_frame = self.handle_nulls_empties_and_duplicates(data_frame)
        # we know that invalid store_type, continent or country_code are all on same rows which are all invalid data, so remove these first
        # we didn't filter on country_code or continent because they have more values or values that are likely to expand in the future
        data_frame = data_frame[data_frame['store_type'].isin(["Mall Kiosk","Super Store","Local","Web Portal","Outlet"])]
        # fix some invalid data in continent field
        mask_continent = data_frame['continent'] == 'eeEurope'
        data_frame.loc[mask_continent, 'continent'] = 'Europe'
        mask_continent = data_frame['continent'] == 'eeAmerica'
        data_frame.loc[mask_continent, 'continent'] = 'America'
        # remove non-numerical characters from float values and set type
        pd.options.mode.chained_assignment = None  # default='warn'
        data_frame['longitude'] = data_frame['longitude'].apply(self.remove_nonnumeric_characters)
        data_frame['longitude'] = data_frame['longitude'].astype('float', errors='ignore')
        data_frame['latitude'] = data_frame['latitude'].apply(self.remove_nonnumeric_characters)
        data_frame['latitude'] = data_frame['latitude'].astype('float', errors='ignore')
        # remove non-numerical characters from int values and set type
        data_frame['staff_numbers'] = data_frame['staff_numbers'].apply(self.remove_nonnumeric_characters)
        data_frame['staff_numbers'] = data_frame['staff_numbers'].astype('int32', errors='raise')
        # standardise date type
        data_frame['opening_date'] = pd.to_datetime(data_frame['opening_date'], format='mixed', errors='ignore')
        pd.options.mode.chained_assignment = 'warn'  # back to default mode
        # re-order so into a more logical order of identification, attributes, location
        data_frame = data_frame[['index', 'store_code', 'store_type', 'staff_numbers', 'opening_date', 'address','locality', 'continent', 'country_code', 'longitude', 'latitude']]
        return data_frame
    
    def reformat_phone_data(self, data_frame, column_name):
        regex_expression = r'^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' #Our regular expression to match
        data_frame.loc[~data_frame[column_name].str.match(regex_expression), column_name] = None # For every row where the column_name column does not match our regular expression, replace the value with None/null
        data_frame[column_name] = data_frame[column_name].replace({r'\+44(0)': '0',r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)

    # Convert a weight to a decimal value represented in kgcard_number
    # Using a 1:1 ratio of ml to g as a rough estimate for the rows containing ml
    def convert_product_weight(self, raw_weight):
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
            return abs(float(new_weight)/divisor)
        except ValueError:
            return None

    # Convert all weights to a decimal value represented in kg
    def convert_product_weights(self, products_data_frame):
        products_data_frame['weight'] = products_data_frame['weight'].apply(self.convert_product_weight)
        products_data_frame.weight = products_data_frame.weight.astype('float')
        return products_data_frame
    
    def clean_products_data(self, data_frame):
        # TODO: more data cleaning
        data_frame = self.convert_product_weights(data_frame)
        data_frame['currency'] = data_frame['product_price'].apply(lambda x: x[:1] if type(x)==str else '£')
        pd.options.mode.chained_assignment = None  # default='warn'
        # rows with no currency symbol are bogus based on our data review so remove them
        regex_expression = r'^[£€\$]' #Our regular expression to match
        data_frame = data_frame.loc[data_frame['currency'].str.match(regex_expression)]
        data_frame['product_price'] = data_frame['product_price'].apply(self.remove_nonnumeric_characters)
        data_frame.product_price = data_frame.product_price.astype('float')
        data_frame['date_added'] = pd.to_datetime(data_frame['date_added'], errors='coerce')
        data_frame = self.handle_nulls_empties_and_duplicates(data_frame)
        data_frame['removed'] = data_frame['removed'].apply(lambda x: True if x is not None and type(x) == str and x.lower() == 'removed' else False)
        data_frame['removed'] = data_frame['removed'].astype('bool')
        pd.options.mode.chained_assignment = 'warn'
        return data_frame

    def clean_orders_data(self, data_frame):
        # remove columns as per specification: first_name, last_name and 1
        data_frame.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        # remove level_0 column which is a duplicate of index
        data_frame.drop(['level_0'], axis=1, inplace=True)
        return self.handle_nulls_empties_and_duplicates(data_frame)

    def clean_time_data(self, data_frame):
        # TODO: more data cleaning
        return self.handle_nulls_empties_and_duplicates(data_frame)
