import logging
import os
import shutil
import zipfile
import pandas as pd

import numpy as np
from flask import Flask, request, jsonify, Blueprint
from flask_restful import Api, Resource
from oracledb import connect
from dotenv import load_dotenv
import os

from config import Config
from processor.health_data_processor import HealthDataProcessor

app = Flask(__name__)
api = Api(app)
app.config.from_object(Config)

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load env variables
load_dotenv()


## Database connection helper
def get_db_connection():
    try:
        return connect(
            user=os.getenv('ORACLE_USER'),
            password=os.getenv('ORACLE_PASSWORD'),
            service_name=os.getenv('ORACLE_SERVICE_NAME'),
            port=1521,
            host=os.getenv('ORACLE_HOST')
        )
    except Exception as e:
        logging.error(f"Error connecting to Oracle DB: {e}")
        raise


# Resource for API version and description
class ApiInfo(Resource):
    def get(self):
        return {
            'version': 'v1',
            'description': 'UzimaSync API. Supports file uploads for health data (ZIP and JSON formats).'
        }, 200


# Resource for file upload and processing
class FileUpload(Resource):
    def post(self):
        # Check if the file is part of the request
        if 'file' not in request.files:
            logging.error("No file part in the request")
            return {'error': 'No file part in the request'}, 400

        file = request.files['file']
        if file.filename == '':
            logging.error("No file selected for uploading")
            return {'error': 'No selected file'}, 400

        if not (file.filename.endswith('.zip') or file.filename.endswith(
                '.json')):
            logging.error("Unsupported file type")
            return {'error': 'File must be a zip or json'}, 400

        try:
            # Handle ZIP and JSON files separately
            if file.filename.endswith('.zip'):
                combined_df = self.handle_zip_file(file)
            else:
                combined_df = self.handle_json_file(file)

            # Save processed data to Oracle DB
            self.save_to_oracle(combined_df)

            return {
                'message': 'Files processed and data saved to the database'}, 200
        except zipfile.BadZipFile:
            logging.error("Invalid ZIP file provided")
            return {'error': 'Invalid zip file'}, 400
        except Exception as e:
            logging.error(f"Error processing file: {e}")
            return {'error': 'Internal server error'}, 500

    # Handle processing of ZIP files
    def handle_zip_file(self, file):
        extract_dir = os.path.join(app.config['UPLOAD_FOLDER'],
                                   'extracted_files')
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)

        zip_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(zip_path)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        processor = HealthDataProcessor(extract_dir)
        combined_df = processor.process_files()

        self.clean_up(zip_path, extract_dir)
        return combined_df

    # Handle processing of JSON files
    def handle_json_file(self, file):
        json_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(json_path)

        processor = HealthDataProcessor(app.config['UPLOAD_FOLDER'])
        processor.process_file(json_path, os.path.splitext(file.filename)[0])
        combined_df = pd.concat(processor.dataframes, ignore_index=True)

        os.remove(json_path)
        return combined_df

    # Save processed data to Oracle database
    def save_to_oracle(self, df):
        with get_db_connection() as connection:
            cursor = connection.cursor()
            try:
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

                df = df.replace({np.nan: None})
                data_to_insert = [tuple(row) for row in
                                  df.itertuples(index=False)]

                cursor.executemany(insert_query, data_to_insert)
                connection.commit()
                logging.info("Data saved to Oracle DB successfully.")
            except Exception as e:
                logging.error(f"Error saving data to Oracle DB: {e}")
                raise
            finally:
                cursor.close()

    # Utility function for cleanup
    def clean_up(self, zip_path, extract_dir):
        try:
            shutil.rmtree(extract_dir)
            os.remove(zip_path)
            logging.info("Cleaned up files successfully.")
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")


# Add resources to the API with versioned endpoints
api.add_resource(ApiInfo, '/api/v1/')
api.add_resource(FileUpload, '/api/v1/upload')

# Run the Flask app
if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5001)))
