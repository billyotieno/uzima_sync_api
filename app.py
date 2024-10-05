import logging
import os
import shutil
import zipfile
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, Blueprint
from flask_restful import Api, Resource
from flask_bcrypt import Bcrypt
from flask_httpauth import HTTPTokenAuth
from oracledb import connect
from dotenv import load_dotenv
import re  # For email validation

from config import Config
from processor.health_data_processor import HealthDataProcessor

# Initialize Flask app and API
app = Flask(__name__)
api = Api(app)
app.config.from_object(Config)

# Initialize Bcrypt and Token Authentication
bcrypt = Bcrypt(app)
auth = HTTPTokenAuth(scheme='Bearer')

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load environment variables
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


## Create a new user with a hashed password and API key, and store email
def create_user(username, password, email):
    connection = get_db_connection()
    cursor = connection.cursor()

    # Hash the password
    password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

    # Generate a unique API key
    api_key = bcrypt.generate_password_hash(username).decode('utf-8')

    try:
        # Check if the email or username already exists
        cursor.execute(
            "SELECT * FROM users WHERE email = :email OR username = :username",
            [email, username])
        if cursor.fetchone():
            return None, 'User with this email or username already exists.'

        # Insert the new user into the Oracle Database
        cursor.execute("""
            INSERT INTO users (username, password_hash, api_key, email)
            VALUES (:username, :password_hash, :api_key, :email)
        """, [username, password_hash, api_key, email])
        connection.commit()

        return api_key, None  # Return the generated API key
    except Exception as e:
        logging.error(f"Error creating user: {e}")
        return None, str(e)
    finally:
        cursor.close()
        connection.close()


## Verify username and password, and return the corresponding API key if valid
def verify_user(username, password):
    connection = get_db_connection()
    cursor = connection.cursor()

    cursor.execute(
        "SELECT password_hash, api_key FROM users WHERE username = :username",
        [username])
    user = cursor.fetchone()

    cursor.close()
    connection.close()

    if user and bcrypt.check_password_hash(user[0], password):
        return user[1]  # Return the API key if password matches
    else:
        return None


## Validate email format using regular expressions
def is_valid_email(email):
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None


## Helper function to find user by API key
def find_user_by_api_key(api_key):
    connection = get_db_connection()
    cursor = connection.cursor()

    cursor.execute("SELECT id, username FROM users WHERE api_key = :api_key", [api_key])
    user = cursor.fetchone()

    cursor.close()
    connection.close()

    if user:
        return {'id': user[0], 'username': user[1]}
    return None


## Authentication route to verify the API key using a token (Bearer scheme)
@auth.verify_token
def verify_api_key(api_key):
    username = find_user_by_api_key(api_key)
    if username:
        return username
    return None


## Resource for API version and description
class ApiInfo(Resource):
    def get(self):
        return {
            'version': 'v1',
            'description': 'UzimaSync API. Supports file uploads for health data (ZIP and JSON formats).'
        }, 200


## Resource for file upload and processing
class FileUpload(Resource):
    @auth.login_required
    def post(self):
        user = auth.current_user()

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
                combined_df = self.handle_zip_file(file, user['id'])  # Pass user_id
            else:
                combined_df = self.handle_json_file(file, user['id'])  # Pass user_id

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
    def handle_zip_file(self, file, user_id):
        extract_dir = os.path.join(app.config['UPLOAD_FOLDER'],
                                   'extracted_files')
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)

        zip_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(zip_path)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        processor = HealthDataProcessor(extract_dir)
        combined_df = processor.process_files(user_id)  # Pass the user_id here

        self.clean_up(zip_path, extract_dir)
        return combined_df

    # Handle processing of JSON files
    def handle_json_file(self, file, user_id):
        json_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(json_path)

        processor = HealthDataProcessor(app.config['UPLOAD_FOLDER'])
        processor.process_file(json_path, user_id)  # Pass the user_id here
        combined_df = pd.concat(processor.dataframes, ignore_index=True)

        os.remove(json_path)
        return combined_df

        # Modified save_to_oracle to accept user_id

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

    def clean_up(self, zip_path, extract_dir):
        try:
            shutil.rmtree(extract_dir)
            os.remove(zip_path)
            logging.info("Cleaned up files successfully.")
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")


## Resource for user registration with email
class UserRegistration(Resource):
    def post(self):
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        email = data.get('email')

        if not username or not password or not email:
            return {'error': 'Username, password, and email are required'}, 400

        if not is_valid_email(email):
            return {'error': 'Invalid email format'}, 400

        api_key, error = create_user(username, password, email)

        if api_key:
            return {'api_key': api_key}, 201
        else:
            return {'error': error}, 500


## Resource for user login
class UserLogin(Resource):
    def post(self):
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')

        if not username or not password:
            return {'error': 'Username and password are required'}, 400

        api_key = verify_user(username, password)

        if api_key:
            return {'api_key': api_key}, 200
        else:
            return {'error': 'Invalid credentials'}, 401


# Add resources to the API with versioned endpoints
api.add_resource(ApiInfo, '/api/v1/')
api.add_resource(FileUpload, '/api/v1/upload')
api.add_resource(UserRegistration, '/api/v1/register')
api.add_resource(UserLogin, '/api/v1/login')

# Run the Flask app
if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5001)))
