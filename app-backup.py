from flask import Flask, request, jsonify
import os
import zipfile
import shutil
from processor.health_data_processor import HealthDataProcessor
from oracledb import connect
from config import Config

app = Flask(__name__)
app.config.from_object(Config)


# Initialize Oracle connection
def get_db_connection():
    return connect(user=app.config['ORACLE_USER'],
                   password=app.config['ORACLE_PASSWORD'],
                   service_name=app.config['ORACLE_SERVICE_NAME'],
                   port=1521,
                   host=app.config['ORACLE_HOST'])


# Route to handle file upload and process the data
@app.route('/upload', methods=['POST'])
def upload_files():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.zip'):
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

        combined_df.head()

        # Store the processed data into the Oracle database
        save_to_oracle(combined_df)

        # Clean up
        shutil.rmtree(extract_dir)
        os.remove(zip_path)

        return jsonify({'message': 'Files processed and data saved to the database'}), 200
    else:
        return jsonify({'error': 'File must be a zip'}), 400


# Save data to Oracle database
def save_to_oracle(df):
    connection = get_db_connection()
    cursor = connection.cursor()

    # Assuming the table is already created with matching columns
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

    import numpy as np

    # Replace NaN values with None (NULL in SQL) and ensure correct data types
    df = df.replace({np.nan: None})

    # Optionally ensure the data types match the database schema
    df['health_data_user'] = df['health_data_user'].astype(str)
    df['type'] = df['type'].astype(str)
    df['date'] = df['date'].astype(str)  # Assuming 'YYYY-MM-DD' format
    df['source'] = df['source'].astype(str)
    df['workout_qty'] = df['workout_qty'].astype(str)  # Or int if applicable
    df['workout_units'] = df['workout_units'].astype(str)
    df['elevation_qty'] = df['elevation_qty'].astype(str)  # Or int if applicable
    df['elevation_units'] = df['elevation_units'].astype(str)
    df['location'] = df['location'].astype(str)
    df['value'] = df['value'].astype(str)
    df['units'] = df['units'].astype(str)
    df['metric_name'] = df['metric_name'].astype(str)

    data_to_insert = [tuple(row) for row in df.itertuples(index=False)]

    cursor.executemany(insert_query, data_to_insert)
    connection.commit()
    cursor.close()
    connection.close()

#     curl -X POST http://127.0.0.1:5000/upload -F "file=@./Dom_HealthAutoExport-2024-08-27-2024-09-26.json"


if __name__ == "__main__":
    app.run(debug=True)
