import pandas as pd
import os

# Define a class to hold automation related data and methods
class Automation:
    
    def __init__(self):
        self.input_path = None
        self.output_path = None
        self.data = None
        self.unique_addresses = None

    # Set the input file path
    def set_input_path(self, input_path):
        self.input_path = input_path

    # Set the output file path
    def set_output_path(self, output_path):
        self.output_path = output_path

    # Load data into the object
    def load_data(self, data):
        self.data = data

    # Set unique addresses in the object
    def set_unique_addresses(self, addresses):
        self.unique_addresses = addresses


# Define a class to hold automation process related methods
class AutomationProcess:

    def __init__(self):
        self.automation = Automation()
        

    # Set the input file path in the Automation object
    def add_input_path(self, input_path):
        self.automation.set_input_path(input_path)
        return self

    # Set the output file path in the Automation object
    def add_output_path(self, output_path):
        self.automation.set_output_path(output_path)
        return self

    # Load data into the Automation object
    def load_dataframe_from_csv(self):
        # Check if the input file path exists and is a valid file
        if not self.automation.input_path or not os.path.isfile(self.automation.input_path):
            raise ValueError("Input path is not set or is not a valid file.")
        # Load the csv file into the object
        self.automation.load_data(pd.read_csv(self.automation.input_path))

    # Standardize the address column in the data
    def standardize_address(self):
        # Convert the address column to lowercase, strip whitespace, and remove extra spaces
        self.automation.data['address'] = self.automation.data['address'].str.lower().str.strip().str.replace('\s+', ' ', regex=True)

    # Set unique addresses in the Automation object
    def get_unique_addresses(self):
        self.automation.set_unique_addresses(self.automation.data['address'].unique())

    # Sort a dataframe by the rating column
    def sort_by_ratings(self, df):
        return df.sort_values(by='Rating', ascending=False)
    
    # Save the data in a csv file
    def save_data(self, df, address):
        filename = f"{self.automation.output_path + address}.csv"
        df.to_csv(filename, index=False)
    
    # Iterate through unique addresses and update categories
    def iterate_addresses(self):
        df = self.automation.data 
        for address in self.automation.unique_addresses:
            # Sort the dataframe by rating for a specific address
            df_filtered = df.loc[df['address'] == address]
            if df_filtered.shape == (1, len(df.columns)):
                # If there is only one row, directly save it to CSV and continue with the next address
                self.save_data(df_filtered, address)
                continue

            # Sort the dataframe by rating for a specific address
            df_sorted = self.sort_by_ratings(df_filtered)
            
            # Set the in_search column to False for all but the first row
            df_sorted.iloc[1:, df_sorted.columns.get_loc('in_search')] = False
            
            # Reset the index to start from 0
            df_sorted = df_sorted.reset_index(drop=True)
            
            # Get the category values for all but the first row
            values = df_sorted['Category1'][1:].tolist()
            
            # Get all the category column names
            columns = [col for col in df_sorted.columns if col.startswith('Category')]
            
            # Update category columns with values from subsequent rows
            for col in columns:
                if not values:
                    break
                if pd.isna(df_sorted.loc[0, col]):
                    df_sorted.loc[0, col] = values.pop(0)
            
            # Save the updated data to a CSV file
            self.save_data(df_sorted, address)
        
        return 'Pipeline Finished Processing'

    # Runs the Automation process
    def automate(self):
        self.load_dataframe_from_csv()
        self.standardize_address()
        self.get_unique_addresses()
        self.iterate_addresses()
        return 'Pipeline Finished Processing'
