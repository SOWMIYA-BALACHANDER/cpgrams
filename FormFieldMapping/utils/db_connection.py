"""
Database Connection Utilities
-----------------------------
Handles PostgreSQL database connections.
"""

import psycopg2
from psycopg2 import sql
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import DB_CONFIG


def get_connection():
    """
    Establish and return a database connection.
    """
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        return conn
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        return None


def test_connection():
    """
    Test the database connection.
    """
    conn = get_connection()
    if conn:
        print("✔ Database connection successful!")
        conn.close()
        return True
    return False
