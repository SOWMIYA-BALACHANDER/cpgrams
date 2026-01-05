"""
Database Connection Utility
---------------------------
Handles PostgreSQL database connections and operations.
"""

import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
import sys
import os
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.db_config import DB_CONFIG, CATEGORY_SCHEMA, CATEGORY_TABLE


class DatabaseConnection:
    """Manages PostgreSQL database connections and category operations."""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=DB_CONFIG["host"],
                port=DB_CONFIG["port"],
                database=DB_CONFIG["database"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"]
            )
            self.connection.autocommit = False  # Use transactions
            self.cursor = self.connection.cursor()
            print("✓ Successfully connected to the database.")
            return True
        except psycopg2.Error as e:
            print(f"✗ Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("✓ Database connection closed.")
    
    def bulk_insert_categories(self, categories: List[Tuple[str, str]], 
                                entity_id: str, 
                                parent_category_id: str = None) -> List[str]:
        """
        Bulk insert multiple categories in a single SQL statement.
        
        Args:
            categories: List of tuples (name, description)
            entity_id: Entity UUID (same for all categories)
            parent_category_id: Parent category UUID (None for top-level)
        
        Returns:
            List of auto-generated UUIDs in the same order as input
        """
        if not categories:
            return []
        
        try:
            # Build VALUES clause for bulk insert
            values_template = "(%s, %s, %s, %s, %s, %s)"
            values_list = []
            params = []
            
            for name, description in categories:
                values_list.append(values_template)
                params.extend([
                    name,
                    description,
                    "OTHER",      # Default category_type
                    "PUBLIC",     # Default visibility_scope
                    entity_id,
                    parent_category_id
                ])
            
            # Construct the bulk insert query
            insert_query = f"""
                INSERT INTO {CATEGORY_SCHEMA}.{CATEGORY_TABLE} 
                (name, description, category_type, visibility_scope, entity_id, parent_category_id)
                VALUES {', '.join(values_list)}
                RETURNING id
            """
            
            self.cursor.execute(insert_query, params)
            
            # Get all auto-generated IDs (in insertion order)
            generated_ids = [row[0] for row in self.cursor.fetchall()]
            return generated_ids
            
        except psycopg2.Error as e:
            print(f"✗ Error bulk inserting categories: {e}")
            self.connection.rollback()
            raise
    
    def commit(self):
        """Commit the current transaction."""
        if self.connection:
            self.connection.commit()
            print("✓ Transaction committed successfully.")
    
    def rollback(self):
        """Rollback the current transaction."""
        if self.connection:
            self.connection.rollback()
            print("✓ Transaction rolled back.")


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    db = DatabaseConnection()
    try:
        if db.connect():
            yield db
        else:
            yield None
    finally:
        db.disconnect()
