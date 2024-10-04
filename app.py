import logging
import os
import shutil
import zipfile
import pandas as pd

import numpy as np
from flask import Flask, request, jsonify
from oracledb import connect

from config import Config
from processor.health_data_processor import HealthDataProcessor

app = Flask(__name__)
app.config.from_object(Config)

# Setup logging
logging.basicConfig(level=logging.INFO)


# Initialize Oracle connection
def get_db_connection():
    try:
        return connect(
            user=app.config['ORACLE_USER'],
            password=app.config['ORACLE_PASSWORD'],
            service_name=app.config['ORACLE_SERVICE_NAME'],
            port=1521,
            host=app.config['ORACLE_HOST']
        )
    except Exception as e:
        logging.error(f"Error connecting to Oracle DB: {e}")
        raise


# Route to handle file upload and process the data
@app.route('/upload', methods=['POST'])
def upload_files():
    # Check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if not (file.filename.endswith('.zip') or file.filename.endswith('.json')):
        return jsonify({'error': 'File must be a zip or json'}), 400

    try:
        if file.filename.endswith('.zip'):
            # Extract zip file
            extract_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'extracted_files')
            if not os.path.exists(extract_dir):
                os.makedirs(extract_dir)

            zip_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(zip_path)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            # Process the extracted files
            processor = HealthDataProcessor(extract_dir)
            combined_df = processor.process_files()

            # Clean up
            clean_up(zip_path, extract_dir)
        else:
            # Process the JSON file directly
            json_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(json_path)

            processor = HealthDataProcessor(app.config['UPLOAD_FOLDER'])
            processor.process_file(json_path, os.path.splitext(file.filename)[0])
            combined_df = pd.concat(processor.dataframes, ignore_index=True)

            # Clean up
            os.remove(json_path)

        # Store the processed data into the Oracle database
        save_to_oracle(combined_df)

        return jsonify({'message': 'Files processed and data saved to the database'}), 200
    except zipfile.BadZipFile:
        return jsonify({'error': 'Invalid zip file'}), 400
    except Exception as e:
        logging.error(f"Error processing file: {e}")
        return jsonify({'error': 'Internal server error'}), 500


# Save data to Oracle database
def save_to_oracle(df):
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Insert query
        insert_query = """
            INSERT INTO health_data (
            health_data_user, 
            type, 
            recorded_date, 
            source, 
            workout_qty, 
            workout_units,
            elevation_qty, 
            elevation_units, 
            location, 
            value, 
            units, 
            metric_name)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)
        """

        # Replace NaN values with None and ensure correct data types
        df = df.replace({np.nan: None})

        # Convert data to required types for database
        df['health_data_user'] = df['health_data_user'].astype(str)
        df['type'] = df['type'].astype(str)
        df['date'] = df['date'].astype(str)
        df['source'] = df['source'].astype(str)
        df['workout_qty'] = df['workout_qty'].astype(str)
        df['workout_units'] = df['workout_units'].astype(str)
        df['elevation_qty'] = df['elevation_qty'].astype(str)
        df['elevation_units'] = df['elevation_units'].astype(str)
        df['location'] = df['location'].astype(str)
        df['value'] = df['value'].astype(str)
        df['units'] = df['units'].astype(str)
        df['metric_name'] = df['metric_name'].astype(str)

        # Prepare data for insertion
        data_to_insert = [tuple(row) for row in df.itertuples(index=False)]

        # Execute insert query
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        logging.info("Data saved to Oracle DB successfully.")
    except Exception as e:
        logging.error(f"Error saving data to Oracle DB: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# Utility function to clean up files and directories
def clean_up(zip_path, extract_dir):
    try:
        shutil.rmtree(extract_dir)
        os.remove(zip_path)
        logging.info("Cleaned up successfully.")
    except Exception as e:
        logging.error(f"Error during cleanup: {e}")


# Start the Flask application
if __name__ == "__main__":
    app.run(debug=True)
