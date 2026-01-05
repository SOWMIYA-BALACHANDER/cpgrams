"""
Hierarchical Category Manager
-----------------------------
Interactive CLI tool for inserting hierarchical category data into PostgreSQL.
Depth-first traversal with yes/no driven subcategory expansion and bulk inserts.

Usage:
    python main.py
"""

import sys
import os
import re
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db_connection import DatabaseConnection


# UUID validation pattern
UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)


def get_valid_uuid(prompt: str) -> str:
    """Get a valid UUID from user input."""
    while True:
        value = input(prompt).strip()
        if UUID_PATTERN.match(value):
            return value
        print("  ⚠ Invalid UUID format. Expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")


def generate_description(category_name: str) -> str:
    """Generate a short description (6-10 words) from the category name."""
    clean_name = category_name.strip().lower()
    
    templates = [
        f"Category for managing {clean_name} related items",
        f"Contains all {clean_name} related entries",
        f"Handles {clean_name} classification and organization",
        f"Groups items related to {clean_name} topic",
        f"Organizes content under {clean_name} classification"
    ]
    
    index = len(clean_name) % len(templates)
    description = templates[index]
    
    words = description.split()
    if len(words) > 10:
        description = " ".join(words[:10])
    
    return description


def get_positive_integer(prompt: str) -> int:
    """Get a positive integer from user input."""
    while True:
        try:
            value = int(input(prompt).strip())
            if value > 0:
                return value
            print("  ⚠ Please enter a positive number.")
        except ValueError:
            print("  ⚠ Invalid input. Please enter a valid number.")


def get_non_empty_string(prompt: str) -> str:
    """Get a non-empty string from user input."""
    while True:
        value = input(prompt).strip()
        if value:
            return value
        print("  ⚠ Input cannot be empty. Please try again.")


def get_yes_no(prompt: str) -> bool:
    """Get a yes/no response from user."""
    while True:
        value = input(prompt).strip().lower()
        if value in ('y', 'yes'):
            return True
        if value in ('n', 'no'):
            return False
        print("  ⚠ Please enter 'y' or 'n'.")


def collect_category_names_multiline(count: int, indent: str) -> List[Tuple[str, str]]:
    """
    Collect category names via multi-line input (paste all at once).
    
    Args:
        count: Expected number of categories
        indent: Indentation for display
    
    Returns:
        List of tuples (name, description)
    """
    while True:
        print(f"{indent}--- Paste {count} category names below (one per line) ---")
        print(f"{indent}--- Press ENTER twice when done ---")
        
        lines = []
        empty_line_count = 0
        
        while True:
            try:
                line = input()
                if line.strip() == "":
                    empty_line_count += 1
                    if empty_line_count >= 1:  # One empty line to finish
                        break
                else:
                    empty_line_count = 0
                    lines.append(line.strip())
            except EOFError:
                break
        
        # Filter out empty lines and get unique non-empty names
        names = [line for line in lines if line]
        
        # Validate count
        if len(names) != count:
            print(f"{indent}⚠ Expected {count} categories, but got {len(names)}.")
            print(f"{indent}  Please re-enter all {count} names.")
            continue
        
        # Generate descriptions and build result
        categories = []
        print(f"{indent}✓ {count} categories parsed:")
        for i, name in enumerate(names, 1):
            description = generate_description(name)
            categories.append((name, description))
            print(f"{indent}  {i}. {name}")
            print(f"{indent}     → {description}")
        
        return categories


def process_subcategories_depth_first(db: DatabaseConnection, entity_id: str,
                                      categories: List[dict], level: int):
    """
    Process subcategories in depth-first order.
    For each category, ask if subcategories exist, then go deep before moving to siblings.
    
    Args:
        db: Database connection
        entity_id: Entity UUID for all categories
        categories: List of dicts with 'id' and 'name' keys
        level: Current depth level
    """
    indent = "  " * level
    
    for idx, cat in enumerate(categories, 1):
        cat_name = cat['name']
        cat_id = cat['id']
        
        print(f"\n{indent}[{idx}/{len(categories)}] Processing: '{cat_name}'")
        
        # Ask if subcategory is available
        has_sub = get_yes_no(f"{indent}Is subcategory available for \"{cat_name}\"? (y/n): ")
        
        if has_sub:
            # Get count of subcategories
            sub_count = get_positive_integer(f"{indent}Enter number of subcategories: ")
            
            # Collect all subcategory names via multi-line input
            sub_categories = collect_category_names_multiline(sub_count, indent)
            
            # Bulk insert all subcategories
            print(f"{indent}Inserting {sub_count} subcategories...")
            sub_ids = db.bulk_insert_categories(
                categories=sub_categories,
                entity_id=entity_id,
                parent_category_id=cat_id
            )
            
            # Create list of inserted subcategories with IDs
            inserted_subs = []
            for i, (name, desc) in enumerate(sub_categories):
                sub_id = sub_ids[i]
                inserted_subs.append({'id': sub_id, 'name': name})
                print(f"{indent}  ✓ Inserted: '{name}' (ID: {sub_id})")
            
            # Recursively process subcategories (depth-first)
            process_subcategories_depth_first(
                db=db,
                entity_id=entity_id,
                categories=inserted_subs,
                level=level + 1
            )
        else:
            print(f"{indent}  → No subcategories for '{cat_name}'")


def main():
    """Main entry point for the category manager."""
    print("\n" + "=" * 60)
    print("   HIERARCHICAL CATEGORY MANAGER")
    print("   Depth-First | Yes/No Driven | Bulk Insert")
    print("=" * 60)
    
    # Initialize database connection
    db = DatabaseConnection()
    
    if not db.connect():
        print("\n✗ Could not establish database connection. Exiting.")
        sys.exit(1)
    
    try:
        # Get entity_id (used for all categories)
        print("\n--- Initial Setup ---")
        entity_id = get_valid_uuid("Enter the Entity ID (UUID format): ")
        print(f"✓ Entity ID set to: {entity_id}")
        
        # Step 1: Get top-level categories
        print("\n" + "=" * 60)
        print("STEP 1: Top-Level Categories")
        print("=" * 60)
        
        top_count = get_positive_integer("Enter number of categories: ")
        
        # Collect all top-level category names via multi-line input
        top_categories = collect_category_names_multiline(top_count, "")
        
        # Bulk insert top-level categories (parent_category_id = NULL)
        print(f"\nInserting {top_count} top-level categories...")
        top_ids = db.bulk_insert_categories(
            categories=top_categories,
            entity_id=entity_id,
            parent_category_id=None
        )
        
        # Create list of inserted categories with IDs
        inserted_top = []
        for i, (name, desc) in enumerate(top_categories):
            cat_id = top_ids[i]
            inserted_top.append({'id': cat_id, 'name': name})
            print(f"  ✓ Inserted: '{name}' (ID: {cat_id})")
        
        # Step 2: Process each top-level category depth-first
        print("\n" + "=" * 60)
        print("STEP 2: Subcategory Expansion (Depth-First)")
        print("=" * 60)
        
        process_subcategories_depth_first(
            db=db,
            entity_id=entity_id,
            categories=inserted_top,
            level=1
        )
        
        # Confirm and commit
        print("\n" + "=" * 60)
        confirm = get_yes_no("Commit all changes to database? (y/n): ")
        
        if confirm:
            db.commit()
            print("\n✓ All categories have been successfully inserted!")
        else:
            db.rollback()
            print("\n✓ All changes have been rolled back. No data saved.")
    
    except KeyboardInterrupt:
        print("\n\n⚠ Operation cancelled by user.")
        db.rollback()
    
    except Exception as e:
        print(f"\n✗ An error occurred: {e}")
        db.rollback()
    
    finally:
        db.disconnect()
        print("\n" + "=" * 60)
        print("   Thank you for using Category Manager!")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
