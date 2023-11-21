class DataCleaning:
    # clean the user data - handle NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
    def clean_user_data(self, raw_data_frame):
        # remove completely empty columns in a copy of the dataframe
        data_frame = raw_data_frame.dropna(how="all", axis=1)
        # TODO: more data cleaning
        # check NULL values
        # check date errors
        # check type issues
        # check data issues
        return data_frame

    # remove any erroneous values, NULL values or errors with formatting.
    def clean_card_data(self, raw_data_frame):
        return self.clean_user_data(raw_data_frame)
    
    def called_clean_store_data(self, raw_data_frame):
        return self.clean_user_data(raw_data_frame)
    
    # Convert a weight to a decimal value represented in kg
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
    
    def clean_products_data(self, raw_data_frame):
        return self.clean_user_data(raw_data_frame)

    def clean_orders_data(self, raw_data_frame):
        # remove columns as per specification: first_name, last_name and 1
        raw_data_frame.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        return self.clean_user_data(raw_data_frame)

    def clean_time_data(self, raw_data_frame):
        return self.clean_user_data(raw_data_frame)
