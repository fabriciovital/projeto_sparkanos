import psycopg2
import pytest
from psycopg2 import OperationalError
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT_POSTGRES'),
    'user': os.getenv('USER_POSTGRES'),
    'password': os.getenv('PASSWORD_POSTGRES'),
    'database': os.getenv('DATABASE')
}

def create_connection():
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database']
        )
        return connection
    except OperationalError as e:
        pytest.fail(f"Error connecting to PostgreSQL: {e}")

# Test to verify the connection
def test_connection():
    connection = create_connection()
    assert connection is not None, "Failed to connect to the database"
    
    # Test if the connection is successful with a simple query
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT NOW();")
            result = cursor.fetchone()
            assert result is not None, "The query to the database failed"
    finally:
        connection.close()
