# check_table.py
from iceberg_catalog import create_iceberg_catalog

def check_and_manage_table():
    catalog = create_iceberg_catalog()
    
    try:
        # Try to load the table
        table = catalog.load_table("manufacturing.quality_inspections")
        print("📋 Table EXISTS:")
        print(f"  Schema fields: {len(table.schema().fields)}")
        
        # Check schema requirements
        schema = table.schema()
        required_fields = [f for f in schema.fields if f.required]
        optional_fields = [f for f in schema.fields if not f.required]
        
        print(f"  Required fields: {len(required_fields)}")
        print(f"  Optional fields: {len(optional_fields)}")
        
        # Show snapshots
        snapshots = list(table.history())
        print(f"  Snapshots: {len(snapshots)}")
        
        # Check if schema matches expected (all required except defect_type)
        if len(required_fields) == 12 and len(optional_fields) == 1:
            print("✅ Schema is CORRECT")
            return "correct"
        else:
            print("❌ Schema is INCORRECT - needs recreation")
            print("   Expected: 12 required + 1 optional field")
            return "incorrect"
            
    except Exception as e:
        print("⚠️  Table does NOT exist")
        print(f"   Error: {e}")
        return "missing"

def delete_table_if_needed():
    catalog = create_iceberg_catalog()
    
    try:
        # Try to drop the table
        catalog.drop_table("manufacturing.quality_inspections")
        print("🗑️  Table DELETED successfully")
        return True
    except Exception as e:
        print(f"❌ Could not delete table: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Checking PyIceberg table status...")
    
    status = check_and_manage_table()
    
    if status == "incorrect":
        print("\n🤔 Table has wrong schema. Delete it? (y/n)")
        choice = input().strip().lower()
        
        if choice == 'y':
            if delete_table_if_needed():
                print("✅ Ready to create new table with correct schema")
            else:
                print("❌ Manual cleanup required")
        else:
            print("ℹ️  Keeping existing table")
    
    elif status == "missing":
        print("✅ Ready to create new table")
    
    else:
        print("✅ Table is ready to use")