import os

class Config:
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')  # Directory for uploaded zip files
    ORACLE_USER = 'dwh_db'
    ORACLE_PASSWORD = 'welcome1234_'
    ORACLE_HOST = '193.122.85.185'
    ORACLE_SERVICE_NAME = 'FREEPDB1'