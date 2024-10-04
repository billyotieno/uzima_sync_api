import oracledb

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

    def test_connection(self):
        """
        Test the connection to the Oracle database.
        :return:
        """
        try:
            # Establish connection to Oracle DB
            connection = oracledb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                service_name=self.service_name
            )
            print("Successfully connected to the Oracle database")

            # Execute a simple query to test
            cursor = connection.cursor()
            cursor.execute("SELECT 'Connection Test Successful' FROM dual")
            result = cursor.fetchone()
            print("Query Result:", result[0])

            # Close the cursor and connection
            cursor.close()
            connection.close()

        except oracledb.DatabaseError as e:
            print(f"Failed to connect to Oracle DB: {str(e)}")


if __name__ == "__main__":
    # Replace the following with your actual Oracle DB credentials and DSN
    oracle_user = "dwh_db"
    oracle_password = "welcome1234_"
    oracle_host = "193.122.85.185"
    oracle_port = 1521
    oracle_service_name = "FREEPDB1"

    # Create an instance of the connection tester
    db_tester = OracleDBConnectionTester(oracle_user, oracle_password, oracle_host, oracle_service_name, oracle_port)

    # Test the connection
    db_tester.test_connection()
