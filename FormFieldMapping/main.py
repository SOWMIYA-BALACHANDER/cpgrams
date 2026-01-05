"""
Form Field Mapping - Interactive CLI
------------------------------------
Maps multiple fields to a form with automatic order_index assignment.
"""

import sys
import os
import re

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.db_connection import get_connection, test_connection
from config.db_config import FORMS_TABLE, FORM_FIELDS_TABLE, FORM_FIELD_MAPPING_TABLE, SCHEMA


def normalize_field_name(name: str) -> str:
    """
    Normalize field name for comparison.
    - Convert to lowercase
    - Remove special characters: / - _ ( ) . , and spaces
    - Trim whitespace
    """
    if not name:
        return ""
    # Remove special characters and spaces
    normalized = re.sub(r'[/\-_().,\s]', '', name.lower().strip())
    return normalized


def validate_form_exists(conn, form_id: str) -> bool:
    """
    Check if the form_id exists in the forms table.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT id FROM {SCHEMA}.{FORMS_TABLE} WHERE id = %s",
            (form_id,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.commit()  # Commit to end transaction block
        return result is not None
    except Exception as e:
        conn.rollback()  # Reset transaction state
        print(f"❌ Error validating form: {e}")
        return False


def get_field_by_id(conn, field_id: str) -> dict:
    """
    Get field details by field_id.
    Returns dict with id and field_name, or None if not found.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT id, field_name FROM {SCHEMA}.{FORM_FIELDS_TABLE} WHERE id = %s",
            (field_id,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.commit()  # Commit to end transaction block
        if result:
            return {"id": result[0], "field_name": result[1]}
        return None
    except Exception as e:
        conn.rollback()  # Reset transaction state
        print(f"❌ Error fetching field by ID: {e}")
        return None


def get_all_fields(conn) -> list:
    """
    Get all fields from form_fields table.
    Returns list of dicts with id, field_name, and normalized_name.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT id, field_name FROM {SCHEMA}.{FORM_FIELDS_TABLE}"
        )
        results = cursor.fetchall()
        cursor.close()
        conn.commit()  # Commit to end transaction block
        
        fields = []
        for row in results:
            fields.append({
                "id": row[0],
                "field_name": row[1],
                "normalized_name": normalize_field_name(row[1])
            })
        return fields
    except Exception as e:
        conn.rollback()  # Reset transaction state
        print(f"❌ Error fetching fields: {e}")
        return []


def resolve_field_by_name(all_fields: list, input_name: str) -> dict:
    """
    Resolve a field by name using normalized comparison.
    Returns matching field dict or None.
    """
    normalized_input = normalize_field_name(input_name)
    
    for field in all_fields:
        if field["normalized_name"] == normalized_input:
            return {
                "id": field["id"],
                "field_name": field["field_name"],
                "input_name": input_name
            }
    return None


def check_existing_mapping(conn, form_id: str, field_id: str) -> bool:
    """
    Check if a mapping already exists for the given form_id and field_id.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT id FROM {SCHEMA}.{FORM_FIELD_MAPPING_TABLE} WHERE form_id = %s AND field_id = %s",
            (form_id, field_id)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.commit()  # Commit to end transaction block
        return result is not None
    except Exception as e:
        conn.rollback()  # Reset transaction state
        print(f"❌ Error checking existing mapping: {e}")
        return False


def insert_mappings(conn, form_id: str, resolved_fields: list) -> bool:
    """
    Insert all mappings in a single transaction.
    Assigns order_index based on input order (1, 2, 3, ...).
    Rolls back on any error.
    """
    try:
        cursor = conn.cursor()
        
        for index, field in enumerate(resolved_fields, start=1):
            cursor.execute(
                f"""
                INSERT INTO {SCHEMA}.{FORM_FIELD_MAPPING_TABLE} (form_id, field_id, order_index)
                VALUES (%s, %s, %s)
                """,
                (form_id, field["id"], index)
            )
        
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error inserting mappings: {e}")
        print("❌ Transaction rolled back. No data was inserted.")
        return False


def get_bulk_input(prompt: str) -> list:
    """
    Get multi-line bulk input from user.
    User presses ENTER twice (empty line) to finish.
    Returns list of non-empty lines.
    """
    print(prompt)
    lines = []
    empty_count = 0
    
    while True:
        try:
            line = input()
            if line.strip() == "":
                empty_count += 1
                if empty_count >= 1:  # Single empty line to finish
                    break
            else:
                empty_count = 0
                lines.append(line.strip())
        except EOFError:
            break
    
    return lines


def display_preview_table(resolved_fields: list, input_mode: int):
    """
    Display the resolved fields preview table.
    """
    print("\n" + "=" * 80)
    print("Resolved Fields Preview")
    print("=" * 80)
    
    if input_mode == 1:  # Field ID mode
        print(f"{'Field ID':<40} | {'Field Name':<35}")
        print("-" * 80)
        for field in resolved_fields:
            field_id_display = str(field["id"])[:36] + "..." if len(str(field["id"])) > 36 else str(field["id"])
            print(f"{field_id_display:<40} | {field['field_name']:<35}")
    else:  # Field Name mode
        print(f"{'Input Name':<25} | {'Matched Field Name':<30} | {'Field ID':<20}")
        print("-" * 80)
        for field in resolved_fields:
            field_id_display = str(field["id"])[:16] + "..." if len(str(field["id"])) > 16 else str(field["id"])
            input_display = field["input_name"][:22] + "..." if len(field["input_name"]) > 22 else field["input_name"]
            matched_display = field["field_name"][:27] + "..." if len(field["field_name"]) > 27 else field["field_name"]
            print(f"{input_display:<25} | {matched_display:<30} | {field_id_display:<20}")
    
    print("=" * 80)


def main():
    """
    Main interactive CLI flow.
    """
    print("\n" + "=" * 60)
    print("       FORM FIELD MAPPING - Interactive CLI")
    print("=" * 60)
    
    # Test database connection
    print("\n🔄 Testing database connection...")
    conn = get_connection()
    if not conn:
        print("❌ Cannot proceed without database connection.")
        return
    print("✔ Database connected successfully!\n")
    
    while True:
        # Step 1: Form Selection
        print("-" * 60)
        form_id = input("Enter Form ID: ").strip()
        
        if not form_id:
            print("❌ Form ID cannot be empty.")
            continue
        
        # Validate form exists
        print("🔄 Validating form...")
        if not validate_form_exists(conn, form_id):
            print(f"❌ Form with ID '{form_id}' does not exist.")
            continue
        print("✔ Form validated successfully!")
        
        # Get number of fields
        try:
            num_fields = int(input("\nEnter number of fields to map: ").strip())
            if num_fields <= 0:
                print("❌ Number of fields must be greater than 0.")
                continue
        except ValueError:
            print("❌ Please enter a valid number.")
            continue
        
        # Step 2: Input Mode Selection
        print("\nHow do you want to provide fields?")
        print("1. Field ID")
        print("2. Field Name")
        
        try:
            input_mode = int(input("Enter your choice (1 or 2): ").strip())
            if input_mode not in [1, 2]:
                print("❌ Invalid choice. Please enter 1 or 2.")
                continue
        except ValueError:
            print("❌ Please enter a valid number.")
            continue
        
        # Step 3: Field Input (Bulk)
        resolved_fields = []
        unresolved_inputs = []
        duplicate_mappings = []
        
        if input_mode == 1:
            # Field ID mode
            print("\n--- Enter field IDs (one per line). Press ENTER twice to finish ---")
            field_inputs = get_bulk_input("")
        else:
            # Field Name mode
            print("\n--- Enter field names (one per line). Press ENTER twice to finish ---")
            field_inputs = get_bulk_input("")
        
        # Filter out blank lines (already done in get_bulk_input)
        field_inputs = [f for f in field_inputs if f]
        
        # Validate input count
        if len(field_inputs) != num_fields:
            print(f"\n❌ Expected {num_fields} fields, but received {len(field_inputs)}.")
            print("Please try again.")
            continue
        
        print("\n🔄 Resolving fields...")
        
        if input_mode == 1:
            # Resolve by Field ID
            for field_input in field_inputs:
                field = get_field_by_id(conn, field_input)
                if field:
                    # Check for duplicate mapping
                    if check_existing_mapping(conn, form_id, field["id"]):
                        duplicate_mappings.append(field_input)
                    else:
                        resolved_fields.append({
                            "id": field["id"],
                            "field_name": field["field_name"],
                            "input_name": field_input
                        })
                else:
                    unresolved_inputs.append(field_input)
        else:
            # Resolve by Field Name
            all_fields = get_all_fields(conn)
            if not all_fields:
                print("❌ Could not fetch fields from database.")
                continue
            
            for field_input in field_inputs:
                field = resolve_field_by_name(all_fields, field_input)
                if field:
                    # Check for duplicate mapping
                    if check_existing_mapping(conn, form_id, field["id"]):
                        duplicate_mappings.append(field_input)
                    else:
                        resolved_fields.append(field)
                else:
                    unresolved_inputs.append(field_input)
        
        # Handle unresolved fields
        if unresolved_inputs:
            print("\n❌ The following fields could not be resolved:")
            for unresolved in unresolved_inputs:
                print(f"   - {unresolved}")
            print("\n❌ Cannot proceed with partial data. Please try again.")
            continue
        
        # Handle duplicate mappings
        if duplicate_mappings:
            print("\n⚠️  The following fields already have mappings for this form:")
            for dup in duplicate_mappings:
                print(f"   - {dup}")
            print("\n❌ Duplicate mappings are not allowed. Please try again.")
            continue
        
        # Display preview
        display_preview_table(resolved_fields, input_mode)
        
        print(f"\n📋 Total fields to map: {len(resolved_fields)}")
        print(f"📋 Form ID: {form_id}")
        print(f"📋 order_index will be assigned: 1 to {len(resolved_fields)}")
        
        # User confirmation
        confirm = input("\nDo you want to continue with these mappings? (y/n): ").strip().lower()
        
        if confirm != 'y':
            print("\n❌ Mapping cancelled by user.")
            retry = input("Do you want to try again? (y/n): ").strip().lower()
            if retry == 'y':
                continue
            else:
                break
        
        # Insert mappings
        print("\n🔄 Inserting mappings...")
        if insert_mappings(conn, form_id, resolved_fields):
            print("\n" + "=" * 60)
            print("✔ Mapping completed successfully!")
            print("✔ order_index assigned automatically (1 to {})".format(len(resolved_fields)))
            print("✔ Transaction committed")
            print("=" * 60)
        
        # Ask if user wants to continue
        another = input("\nDo you want to map another form? (y/n): ").strip().lower()
        if another != 'y':
            break
    
    # Close connection
    conn.close()
    print("\n👋 Goodbye!")


if __name__ == "__main__":
    main()
