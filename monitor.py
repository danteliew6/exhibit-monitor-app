import csv
import os
import time
import csv
import xml.etree.ElementTree as ET
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import mysql.connector

# Function to determine the file type
def determine_file_type(csv_file_path):
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        
        # Read the first row of the CSV file
        header = next(reader)
        
        # Check if the header contains specific column names or values
        if 'Customer Id' in header:
            return 'fileTypeA'
        elif 'Stock' in header:
            return 'fileTypeB'
        # Add more conditions to match your file types
        
        # If none of the conditions match, return an unknown file type
        return 'unknown'

# Custom event handler for file changes
class FileHandler(FileSystemEventHandler):
    def __init__(self, config):
        super(FileHandler, self).__init__()
        self.config = config  # Store the parsed XML configuration
        self.db_config = {
            'host': config.find("database/connection/host").text,
            'port': int(config.find("database/connection/port").text),
            'database': config.find("database/connection/databaseName").text,
            'user': config.find("database/connection/username").text,
            'password': config.find("database/connection/password").text
        }

    def is_file_valid(self, file_path):
        # Your file validation logic here
        pass

    def insert_valid_record(self, cursor, file_path):
        # Get the record count
        record_count = 0
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                record_count += 1
        
        # Get the current date and time
        current_date = time.strftime('%Y-%m-%d')
        current_time = time.strftime('%H:%M:%S')

        # Insert the valid record into the database
        query = "INSERT INTO valid_records (filename, record_count, processed_date, processed_time) VALUES (%s, %s, %s, %s)"
        values = (os.path.basename(file_path), record_count, current_date, current_time)
        cursor.execute(query, values)

    def insert_invalid_record(self, cursor, file_path, error_message):
        # Get the current date and time
        current_date = time.strftime('%Y-%m-%d')
        current_time = time.strftime('%H:%M:%S')

        # Insert the invalid record into the database
        query = "INSERT INTO invalid_records (filename, error_message, processed_date, processed_time) VALUES (%s, %s, %s, %s)"
        values = (os.path.basename(file_path), error_message, current_date, current_time)
        cursor.execute(query, values)

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            file_type = determine_file_type(file_path)
            grace_period = int(self.config.find(f"fileProcessing/gracePeriod/{file_type}").text)

            # Check if the file is in knowledge of the application and within the grace period
            if file_type is not None and grace_period is not None:
                current_time = time.time()
                file_modified_time = os.path.getmtime(file_path)
                if current_time - file_modified_time <= grace_period * 60:
                    print(f"Processing file: {file_path}")
                    # Perform the processing logic for the file
                    # Insert data into the database or perform any required operations

                    try:
                        # Connect to the MySQL database
                        connection = mysql.connector.connect(**self.db_config)
                        cursor = connection.cursor()

                        # Insert data into the appropriate table based on validity
                        if self.is_file_valid(file_path):
                            self.insert_valid_record(cursor, file_path)
                        else:
                            error_message = "Invalid file format"  # Update with your error message
                            self.insert_invalid_record(cursor, file_path, error_message)

                        connection.commit()
                        print("File processed and data inserted into the database")
                    except mysql.connector.Error as error:
                        print("Error inserting data into the database:", error)

                    # Move the processed file to the processed directory
                    processed_directory = self.config.find("fileProcessing/processedDirectory").text
                    processed_file_path = os.path.join(processed_directory, os.path.basename(file_path))
                    os.rename(file_path, processed_file_path)
                    print(f"File moved to: {processed_file_path}")


# Parse the XML configuration file
config_file_path = os.getcwd() + '/monitor.xml'
tree = ET.parse(config_file_path)
config = tree.getroot()

# Get the input directory from the XML configuration
input_directory = config.find("fileProcessing/inputDirectory").text

# Create a file system event handler and observer
event_handler = FileHandler(config)
observer = Observer()
observer.schedule(event_handler, input_directory, recursive=False)
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()

observer.join()
# # Example usage
# csv_file_path = '/path/to/your/file.csv'
# file_type = determine_file_type(csv_file_path)
# print(f"The file type is: {file_type}")

