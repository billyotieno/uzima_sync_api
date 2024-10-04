import os

import oracledb
import pandas as pd


class OracleDBConnectionTester:
    """
    A simple class to test the connection to an Oracle database.
    """

    def __init__(self, user, password, host, service_name, port=1521):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name

    def get_db_connection(self):
        """
        Establishes a connection to the Oracle DB and returns the connection object.
        """
        try:
            connection = oracledb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                service_name=self.service_name
            )
            return connection
        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Failed to connect to Oracle DB: {error.message}")
            return None

    def test_connection(self):
        """
        Test the connection to the Oracle database.
        :return:
        """
        connection = self.get_db_connection()
        if connection:
            print("Successfully connected to the Oracle database")

            # Execute a simple query to test
            cursor = connection.cursor()
            cursor.execute("SELECT 'Connection Test Successful' FROM dual")
            result = cursor.fetchone()
            print("Query Result:", result[0])

            # Close the cursor and connection
            cursor.close()
            connection.close()


def save_to_oracle(df, connection):
    """
    Save DataFrame data to the Oracle database.
    """
    cursor = connection.cursor()

    # Assuming the table is already created with matching columns
    insert_query = """
        INSERT INTO health_data (
        health_data_user, 
        type, 
        recorded_date, 
        source, 
        step_qty, 
        step_units,
        elevation_qty, 
        elevation_units, 
        location, 
        value, 
        units, 
        metric_name)
        VALUES (:1, :2, to_date(:3,'YYYY-MM-DD'), :4, :5, :6, :7, :8, :9, :10, :11, :12)
    """

    # Convert DataFrame rows into tuples for insertion
    data_to_insert = [tuple(row) for row in df.itertuples(index=False)]
    print(data_to_insert)

    cursor.executemany(insert_query, data_to_insert)
    connection.commit()
    cursor.close()
    print("Data inserted successfully.")


if __name__ == "__main__":
    # Use environment variables for credentials (if available)
    oracle_user = os.getenv('ORACLE_USER', 'dwh_db')
    oracle_password = os.getenv('ORACLE_PASSWORD', 'welcome1234_')
    oracle_host = os.getenv('ORACLE_HOST', '193.122.85.185')
    oracle_port = int(os.getenv('ORACLE_PORT', 1521))
    oracle_service_name = os.getenv('ORACLE_SERVICE_NAME', 'FREEPDB1')

    # Create an instance of the connection tester
    db_tester = OracleDBConnectionTester(oracle_user, oracle_password, oracle_host, oracle_service_name)

    # Test the connection
    db_tester.test_connection()

    # Mock some data in a DataFrame to insert into the Oracle database
    data = {
        'health_data_user': ['user1', 'user2'],
        'type': ['step', 'elevation'],
        'date': ['2024-10-01', '2024-10-02'],
        'source': ['wearable1', 'wearable2'],
        'step_qty': [1000, 100],
        'step_units': ['steps', 'steps'],
        'elevation_qty': [100, 30],
        'elevation_units': ['counts', 'meters'],
        'location': ['Nairobi', 'Mombasa'],
        'value': [1000, 30],
        'units': ['steps', 'meters'],
        'metric_name': ['steps_taken', 'elevation_gain']
    }

    df = pd.DataFrame(data)

    # Get connection from the tester class
    connection = db_tester.get_db_connection()

    # Insert DataFrame into the Oracle DB
    if connection:
        save_to_oracle(df, connection)
        connection.close()
