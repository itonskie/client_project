import pandas as pd
import configparser
import os

# Define a class to hold automation related data and methods
class Automation:
    """
    Holds input and output file paths, the data itself, and unique addresses.
    Includes methods to set input and output paths, load data, and set unique addresses.
    """
    def __init__(self, input_path=None, output_path=None, config_path=None):
        self.input_path = None
        self.output_path = None
        self.data = None
        self.unique_addresses = None

        if config_path:
            # Read the configuration file
            config = configparser.ConfigParser()
            config.read(config_path)

            # Set the input and output paths if they are specified in the configuration file
            self.input_path = config['Paths'].get('input_path', self.input_path)
            self.output_path = config['Paths'].get('output_path', self.output_path)

    def set_input_path(self, input_path):
        """Set the input file path."""
        self.input_path = input_path

    def set_output_path(self, output_path):
        """Set the output file path."""
        self.output_path = output_path

    def load_data(self, data):
        """Load data into the object."""
        self.data = data

    def set_unique_addresses(self, addresses):
        """Set unique addresses in the object."""
        self.unique_addresses = addresses


class AutomationProcess:
    """
    Creates an instance of Automation.
    Includes methods to set input and output file paths, load data, standardize addresses,
    get unique addresses, sort DataFrames by rating and rating count, save data to CSV files,
    write data to a CSV file with a header, iterate through unique addresses, and automate the entire process.
    """
    def __init__(self, input_path=None, output_path=None, config_path=None):
        self.automation = Automation(config_path=config_path)
        if input_path:
            self.automation.set_input_path(input_path)
        if output_path:
            self.automation.set_output_path(output_path)

    def add_input_path(self, input_path):
        """Set the input file path in the Automation object."""
        self.automation.set_input_path(input_path)
        return self

    def add_output_path(self, output_path):
        """Set the output file path in the Automation object."""
        self.automation.set_output_path(output_path)
        return self

    def load_dataframe_from_csv(self):
        """Load data from CSV files into a pandas DataFrame."""
        if not self.automation.input_path or not os.path.isfile(self.automation.input_path):
            raise ValueError("Input path is not set or is not a valid file.")
        self.automation.load_data(pd.read_csv(self.automation.input_path))

    def standardize_address(self):
        """Standardize the address column in the data."""
        self.automation.data['address'] = self.automation.data['address'].str.lower().str.strip().str.replace('\s+', ' ', regex=True)

    def get_unique_addresses(self):
        """Get unique addresses."""
        self.automation.set_unique_addresses(self.automation.data['address'].unique())

    def sort_by_ratings(self, df):
        """Sort a dataframe by the rating column."""
        return df.sort_values(by='Rating', ascending=False)

    def sort_by_rating_count(self, df):
        """Sort a dataframe by the rating count column."""
        return df.sort_values(by='Rating Count', ascending=False)

    def save_data(self, df, address):
        """Save the data in a csv file."""
        filename = f"{self.automation.output_path + address}.csv"
        df.to_csv(filename, index=False)

    def write_dataframe(self, df, address):
        """
        Write a dataframe to the CSV file with its name as a header.
        """
        # Create the _total directory if it doesn't exist
        total_directory = os.path.join(self.automation.output_path, '_total')
        os.makedirs(total_directory, exist_ok=True)

        # Define the output file path within the _total directory
        output_file_path = os.path.join(total_directory, 'output.csv')

        # Open a file object in append mode
        with open(output_file_path, 'a') as f:
            # Write the first dataframe to the file
            if f.tell() != 0:
                f.write('\n')
            f.write(f'"{address}"\n')
            df.to_csv(f, index=False)

    def iterate_addresses_sorted_by_rating_count(self):
        """
        Iterate through unique addresses and update categories based on rating count.
        """
        df = self.automation.data 
        for address in self.automation.unique_addresses:
            # Sort the dataframe by rating count for a specific address
            df_filtered = df.loc[df['address'] == address]
            if df_filtered.shape == (1, len(df.columns)):
                # If there is only one row, directly save it to CSV and continue with the next address
                self.save_data(df_filtered, address)
                self.write_dataframe(df_filtered, address)
                continue

            # Sort the dataframe by rating count for a specific address
            df_sorted = self.sort_by_rating_count(df_filtered)
            
            # Set the in_search column to False for all but the first row
            df_sorted.iloc[1:, df_sorted.columns.get_loc('in_search')] = False
            
            # Reset the index to start from 0
            df_sorted = df_sorted.reset_index(drop=True)
            
            # Get the category values for all but the first row
            values = list(set(df_sorted['Category1'][1:].tolist()))

            # Remove the first value from the set, if it exists
            first_value = df_sorted.loc[0, 'Category1']
            if first_value in values:
                values.remove(first_value)

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
            self.write_dataframe(df_sorted, address)

        return 'Pipeline Finished Processing'

    def automate_by_rating_count(self):
        """
        Runs the Automation process.
        """
        self.load_dataframe_from_csv()
        self.standardize_address()
        self.get_unique_addresses()
        self.iterate_addresses_sorted_by_rating_count()
        return 'Pipeline Finished Processing'

    # Iterate through unique addresses and update categories
    def iterate_addresses_by_ratings(self):
        df = self.automation.data 
        for address in self.automation.unique_addresses:
            # Sort the dataframe by rating for a specific address
            df_filtered = df.loc[df['address'] == address]
            if df_filtered.shape == (1, len(df.columns)):
                # If there is only one row, directly save it to CSV and continue with the next address
                self.save_data(df_filtered, address)
                self.write_dataframe(df_filtered, address)
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
            self.write_dataframe(df_sorted, address)
        
        return 'Pipeline Finished Processing'

    def automate_by_ratings(self):
        """
        Runs the Automation process.
        """
        self.load_dataframe_from_csv()
        self.standardize_address()
        self.get_unique_addresses()
        self.iterate_addresses_by_ratings()
        return 'Pipeline Finished Processing'
