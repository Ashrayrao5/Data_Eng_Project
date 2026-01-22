# """
# PostgreSQL Data Loader for Power BI Integration
# Loads cleaned CSV data into PostgreSQL dataeng_project schema
# Ready for Power BI dashboard connection
#
# SETUP:
# 1. Install psycopg2: pip install psycopg2-binary
# 2. Update DB_CONFIG with your PostgreSQL credentials
# 3. Place CSV files in same directory as this script
# 4. Run this script: python load_to_postgres.py
# """
#
# import csv
# import psycopg2
# from datetime import datetime, date
#
# # ==========================================
# # DATABASE CONFIGURATION - UPDATE THESE!
# # ==========================================
#
# DB_CONFIG = {
#     'host': 'localhost',
#     'port': '5432',
#     'database': 'postgres',
#     'user': 'postgres',
#     'password': 'Ashrayrao@15',
#     'schema': 'dataeng_project'
# }
#
# # CSV file paths - update if needed
# CSV_FILES = {
#     'sales': 'cleaned_sales_data.csv',
#     'students': 'cleaned_student_data.csv'
# }
#
# # ==========================================
# # HELPER FUNCTIONS
# # ==========================================
#
# def load_csv(file_path):
#     """Load CSV file into list of dictionaries"""
#     try:
#         with open(file_path, 'r', encoding='utf-8') as f:
#             reader = csv.DictReader(f)
#             return list(reader)
#     except FileNotFoundError:
#         print(f"❌ File not found: {file_path}")
#         return []
#     except Exception as e:
#         print(f"❌ Error loading {file_path}: {e}")
#         return []
#
#
# def safe_int(value):
#     """Convert to integer, return None if invalid"""
#     try:
#         return int(value) if value and str(value).strip() else None
#     except:
#         return None
#
#
# def safe_float(value):
#     """Convert to float, return None if invalid"""
#     try:
#         return float(value) if value and str(value).strip() else None
#     except:
#         return None
#
#
# def safe_date(value):
#     """Convert to date, return None if invalid"""
#     if not value or str(value).strip() == '':
#         return None
#     try:
#         return datetime.strptime(str(value), '%Y-%m-%d').date()
#     except:
#         return None
#
#
# def safe_bool(value):
#     """Convert to boolean"""
#     if isinstance(value, bool):
#         return value
#     if isinstance(value, str):
#         return value.lower() in ('true', '1', 't', 'yes')
#     return bool(value)
#
#
# # ==========================================
# # DATABASE SETUP FUNCTIONS
# # ==========================================
#
# def get_connection():
#     """Connect to PostgreSQL database"""
#     try:
#         conn = psycopg2.connect(
#             host=DB_CONFIG['host'],
#             port=DB_CONFIG['port'],
#             database=DB_CONFIG['database'],
#             user=DB_CONFIG['user'],
#             password=DB_CONFIG['password']
#         )
#         print(f"✓ Connected to PostgreSQL database '{DB_CONFIG['database']}'")
#         return conn
#     except Exception as e:
#         print(f"❌ Database connection failed: {e}")
#         print("\n⚠️  Please check:")
#         print("   1. PostgreSQL is running")
#         print("   2. Database credentials in DB_CONFIG are correct")
#         print("   3. Database exists")
#         return None
#
#
# def create_schema_and_tables(conn):
#     """Create schema and all tables"""
#     cursor = conn.cursor()
#     schema = DB_CONFIG['schema']
#
#     try:
#         print(f"\n[1] Creating schema and tables...")
#
#         # Create schema
#         cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
#         print(f"✓ Schema '{schema}' created")
#
#         # Drop existing tables (fresh start)
#         cursor.execute(f"DROP TABLE IF EXISTS {schema}.fact_inventory CASCADE;")
#         cursor.execute(f"DROP TABLE IF EXISTS {schema}.fact_enrollment CASCADE;")
#         cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_item CASCADE;")
#         cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_supplier CASCADE;")
#         cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_category CASCADE;")
#         cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_student CASCADE;")
#         cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_major CASCADE;")
#
#         # Create Sales Dimension Tables
#
#         cursor.execute(f"""
#             CREATE TABLE {schema}.dim_item (
#                 item_id INT PRIMARY KEY,
#                 item_name TEXT,
#                 category TEXT
#             );
#         """)
#         print("✓ Created table: dim_item")
#
#         cursor.execute(f"""
#             CREATE TABLE {schema}.dim_supplier (
#                 supplier_id SERIAL PRIMARY KEY,
#                 supplier_name TEXT UNIQUE
#             );
#         """)
#         print("✓ Created table: dim_supplier")
#
#         cursor.execute(f"""
#             CREATE TABLE {schema}.dim_category (
#                 category_id SERIAL PRIMARY KEY,
#                 category_name TEXT UNIQUE
#             );
#         """)
#         print("✓ Created table: dim_category")
#
#         # Create Sales Fact Table
#
#         cursor.execute(f"""
#             CREATE TABLE {schema}.fact_inventory (
#                 inventory_id SERIAL PRIMARY KEY,
#                 item_id INT,
#                 supplier_id INT,
#                 category_id INT,
#                 date_added DATE,
#                 quantity INT,
#                 price NUMERIC(10,2),
#                 total_value NUMERIC(10,2),
#                 has_valid_date BOOLEAN,
#                 has_valid_price BOOLEAN,
#                 FOREIGN KEY (item_id) REFERENCES {schema}.dim_item(item_id),
#                 FOREIGN KEY (supplier_id) REFERENCES {schema}.dim_supplier(supplier_id),
#                 FOREIGN KEY (category_id) REFERENCES {schema}.dim_category(category_id)
#             );
#         """)
#         print("✓ Created table: fact_inventory")
#
#         # Create Student Dimension Tables
#
#         cursor.execute(f"""
#             CREATE TABLE {schema}.dim_student (
#                 student_id INT PRIMARY KEY,
#                 name TEXT,
#                 age INT,
#                 gender TEXT
#             );
#         """)
#         print("✓ Created table: dim_student")
#
#         cursor.execute(f"""
#             CREATE TABLE {schema}.dim_major (
#                 major_id SERIAL PRIMARY KEY,
#                 major_name TEXT UNIQUE
#             );
#         """)
#         print("✓ Created table: dim_major")
#
#         # Create Student Fact Table
#
#         cursor.execute(f"""
#             CREATE TABLE {schema}.fact_enrollment (
#                 enrollment_id SERIAL PRIMARY KEY,
#                 student_id INT,
#                 major_id INT,
#                 grade TEXT,
#                 enrollment_date DATE,
#                 days_enrolled INT,
#                 has_valid_age BOOLEAN,
#                 has_valid_enrollment_date BOOLEAN,
#                 FOREIGN KEY (student_id) REFERENCES {schema}.dim_student(student_id),
#                 FOREIGN KEY (major_id) REFERENCES {schema}.dim_major(major_id)
#             );
#         """)
#         print("✓ Created table: fact_enrollment")
#
#         conn.commit()
#         print(f"✓ All tables created successfully in schema '{schema}'")
#
#     except Exception as e:
#         conn.rollback()
#         print(f"❌ Error creating tables: {e}")
#         raise
#     finally:
#         cursor.close()
#
#
# # ==========================================
# # DATA LOADING FUNCTIONS
# # ==========================================
#
# def load_sales_data(conn, csv_file):
#     """Load sales data into dimension and fact tables"""
#     cursor = conn.cursor()
#     schema = DB_CONFIG['schema']
#
#     print(f"\n[2] Loading sales data from {csv_file}...")
#
#     # Load CSV
#     sales_data = load_csv(csv_file)
#     if not sales_data:
#         print("❌ No sales data to load")
#         return
#
#     try:
#         # Track unique values for dimensions
#         supplier_lookup = {}
#         category_lookup = {}
#         loaded_items = set()
#
#         # Load dimension tables first
#         print("   Loading dimension tables...")
#
#         for row in sales_data:
#             item_id = safe_int(row.get('item_id'))
#             if not item_id:
#                 continue
#
#             # DIM_ITEM (deduplicated)
#             if item_id not in loaded_items:
#                 cursor.execute(f"""
#                     INSERT INTO {schema}.dim_item (item_id, item_name, category)
#                     VALUES (%s, %s, %s)
#                     ON CONFLICT (item_id) DO NOTHING
#                 """, (
#                     item_id,
#                     row.get('item_name'),
#                     row.get('category')
#                 ))
#                 loaded_items.add(item_id)
#
#             # DIM_SUPPLIER
#             supplier = row.get('supplier')
#             if supplier and supplier not in supplier_lookup:
#                 cursor.execute(f"""
#                     INSERT INTO {schema}.dim_supplier (supplier_name)
#                     VALUES (%s)
#                     ON CONFLICT (supplier_name) DO NOTHING
#                     RETURNING supplier_id
#                 """, (supplier,))
#                 result = cursor.fetchone()
#                 if result:
#                     supplier_lookup[supplier] = result[0]
#                 else:
#                     # If conflict, fetch existing ID
#                     cursor.execute(f"""
#                         SELECT supplier_id FROM {schema}.dim_supplier
#                         WHERE supplier_name = %s
#                     """, (supplier,))
#                     supplier_lookup[supplier] = cursor.fetchone()[0]
#
#             # DIM_CATEGORY
#             category = row.get('category')
#             if category and category not in category_lookup:
#                 cursor.execute(f"""
#                     INSERT INTO {schema}.dim_category (category_name)
#                     VALUES (%s)
#                     ON CONFLICT (category_name) DO NOTHING
#                     RETURNING category_id
#                 """, (category,))
#                 result = cursor.fetchone()
#                 if result:
#                     category_lookup[category] = result[0]
#                 else:
#                     cursor.execute(f"""
#                         SELECT category_id FROM {schema}.dim_category
#                         WHERE category_name = %s
#                     """, (category,))
#                     category_lookup[category] = cursor.fetchone()[0]
#
#         print(f"   ✓ Loaded {len(loaded_items)} items")
#         print(f"   ✓ Loaded {len(supplier_lookup)} suppliers")
#         print(f"   ✓ Loaded {len(category_lookup)} categories")
#
#         # Load fact table
#         print("   Loading fact table...")
#         fact_count = 0
#
#         for row in sales_data:
#             item_id = safe_int(row.get('item_id'))
#             if not item_id:
#                 continue
#
#             supplier_id = supplier_lookup.get(row.get('supplier'))
#             category_id = category_lookup.get(row.get('category'))
#
#             cursor.execute(f"""
#                 INSERT INTO {schema}.fact_inventory (
#                     item_id, supplier_id, category_id, date_added,
#                     quantity, price, total_value, has_valid_date, has_valid_price
#                 ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#             """, (
#                 item_id,
#                 supplier_id,
#                 category_id,
#                 safe_date(row.get('date_added')),
#                 safe_int(row.get('quantity')),
#                 safe_float(row.get('price')),
#                 safe_float(row.get('total_value')),
#                 safe_bool(row.get('has_valid_date')),
#                 safe_bool(row.get('has_valid_price'))
#             ))
#             fact_count += 1
#
#         conn.commit()
#         print(f"   ✓ Loaded {fact_count} records into fact_inventory")
#
#     except Exception as e:
#         conn.rollback()
#         print(f"❌ Error loading sales data: {e}")
#         raise
#     finally:
#         cursor.close()
#
#
# def load_student_data(conn, csv_file):
#     """Load student data into dimension and fact tables"""
#     cursor = conn.cursor()
#     schema = DB_CONFIG['schema']
#
#     print(f"\n[3] Loading student data from {csv_file}...")
#
#     # Load CSV
#     student_data = load_csv(csv_file)
#     if not student_data:
#         print("❌ No student data to load")
#         return
#
#     try:
#         # Track unique majors
#         major_lookup = {}
#         loaded_students = set()
#
#         # Load dimension tables
#         print("   Loading dimension tables...")
#
#         for row in student_data:
#             student_id = safe_int(row.get('student_id'))
#             if not student_id:
#                 continue
#
#             # DIM_STUDENT (deduplicated)
#             if student_id not in loaded_students:
#                 cursor.execute(f"""
#                     INSERT INTO {schema}.dim_student (student_id, name, age, gender)
#                     VALUES (%s, %s, %s, %s)
#                     ON CONFLICT (student_id) DO NOTHING
#                 """, (
#                     student_id,
#                     row.get('name'),
#                     safe_int(row.get('age')),
#                     row.get('gender')
#                 ))
#                 loaded_students.add(student_id)
#
#             # DIM_MAJOR
#             major = row.get('major')
#             if major and major not in major_lookup:
#                 cursor.execute(f"""
#                     INSERT INTO {schema}.dim_major (major_name)
#                     VALUES (%s)
#                     ON CONFLICT (major_name) DO NOTHING
#                     RETURNING major_id
#                 """, (major,))
#                 result = cursor.fetchone()
#                 if result:
#                     major_lookup[major] = result[0]
#                 else:
#                     cursor.execute(f"""
#                         SELECT major_id FROM {schema}.dim_major
#                         WHERE major_name = %s
#                     """, (major,))
#                     major_lookup[major] = cursor.fetchone()[0]
#
#         print(f"   ✓ Loaded {len(loaded_students)} students")
#         print(f"   ✓ Loaded {len(major_lookup)} majors")
#
#         # Load fact table
#         print("   Loading fact table...")
#         fact_count = 0
#
#         for row in student_data:
#             student_id = safe_int(row.get('student_id'))
#             if not student_id:
#                 continue
#
#             major_id = major_lookup.get(row.get('major'))
#
#             cursor.execute(f"""
#                 INSERT INTO {schema}.fact_enrollment (
#                     student_id, major_id, grade, enrollment_date,
#                     days_enrolled, has_valid_age, has_valid_enrollment_date
#                 ) VALUES (%s, %s, %s, %s, %s, %s, %s)
#             """, (
#                 student_id,
#                 major_id,
#                 row.get('grade'),
#                 safe_date(row.get('enrollment_date')),
#                 safe_int(row.get('days_enrolled')),
#                 safe_bool(row.get('has_valid_age')),
#                 safe_bool(row.get('has_valid_enrollment_date'))
#             ))
#             fact_count += 1
#
#         conn.commit()
#         print(f"   ✓ Loaded {fact_count} records into fact_enrollment")
#
#     except Exception as e:
#         conn.rollback()
#         print(f"❌ Error loading student data: {e}")
#         raise
#     finally:
#         cursor.close()
#
#
# def verify_data(conn):
#     """Verify data was loaded correctly"""
#     cursor = conn.cursor()
#     schema = DB_CONFIG['schema']
#
#     print(f"\n[4] Verifying data...")
#
#     tables = [
#         'dim_item', 'dim_supplier', 'dim_category',
#         'fact_inventory', 'dim_student', 'dim_major',
#         'fact_enrollment'
#     ]
#
#     for table in tables:
#         cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
#         count = cursor.fetchone()[0]
#         print(f"   {table}: {count} records")
#
#     cursor.close()
#
#
# def print_connection_info():
#     """Print connection information for Power BI"""
#     print("\n" + "=" * 70)
#     print("✅ DATA LOADED SUCCESSFULLY!")
#     print("=" * 70)
#     print("\n📊 POWER BI CONNECTION INFORMATION:")
#     print("-" * 70)
#     print(f"Server: {DB_CONFIG['host']}")
#     print(f"Database: {DB_CONFIG['database']}")
#     print(f"Port: {DB_CONFIG['port']}")
#     print(f"Schema: {DB_CONFIG['schema']}")
#     print(f"User: {DB_CONFIG['user']}")
#     print("-" * 70)
#     print("\n📋 TABLES AVAILABLE FOR POWER BI:")
#     print("   Sales/Inventory:")
#     print("   • dataeng_project.dim_item")
#     print("   • dataeng_project.dim_supplier")
#     print("   • dataeng_project.dim_category")
#     print("   • dataeng_project.fact_inventory")
#     print("\n   Students:")
#     print("   • dataeng_project.dim_student")
#     print("   • dataeng_project.dim_major")
#     print("   • dataeng_project.fact_enrollment")
#     print("-" * 70)
#
#
# # ==========================================
# # MAIN EXECUTION
# # ==========================================
#
# def main():
#     """Main execution function"""
#     print("=" * 70)
#     print("PostgreSQL Data Loader for Power BI")
#     print("=" * 70)
#
#     # Connect to database
#     conn = get_connection()
#     if not conn:
#         return
#
#     try:
#         # Create schema and tables
#         create_schema_and_tables(conn)
#
#         # Load sales data
#         load_sales_data(conn, CSV_FILES['sales'])
#
#         # Load student data
#         load_student_data(conn, CSV_FILES['students'])
#
#         # Verify data
#         verify_data(conn)
#
#         # Print connection info
#         print_connection_info()
#
#     except Exception as e:
#         print(f"\n❌ Error: {e}")
#         print("\nPlease check the error message above and try again.")
#     finally:
#         conn.close()
#         print("\n✓ Database connection closed")
#
#
# if __name__ == "__main__":
#     main()


"""
PostgreSQL Data Loader for Power BI Integration
Loads cleaned CSV data from output_files directory into PostgreSQL dataeng_project schema
Ready for Power BI dashboard connection

SETUP:
1. Install psycopg2: pip install psycopg2-binary
2. Update DB_CONFIG with your PostgreSQL credentials
3. Run ETL_project.py first to generate CSV files in output_files/
4. Run this script: python load_to_postgres.py
"""

import csv
import psycopg2
import os
from datetime import datetime, date

# ==========================================
# DATABASE CONFIGURATION - UPDATE THESE!
# ==========================================

DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Ashrayrao@15',
    'schema': 'dataeng_project'
}

# CSV file paths - now reading from output_files directory
OUTPUT_DIR = 'output_files'
CSV_FILES = {
    'sales': os.path.join(OUTPUT_DIR, 'cleaned_sales_data.csv'),
    'students': os.path.join(OUTPUT_DIR, 'cleaned_student_data.csv')
}


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def check_csv_files():
    """Check if required CSV files exist in output_files directory"""
    missing_files = []

    if not os.path.exists(OUTPUT_DIR):
        print(f"❌ Directory '{OUTPUT_DIR}' not found!")
        print(f"   Please run ETL_project.py first to generate the output files.")
        return False

    for key, filepath in CSV_FILES.items():
        if not os.path.exists(filepath):
            missing_files.append(filepath)

    if missing_files:
        print(f"❌ Missing CSV files:")
        for f in missing_files:
            print(f"   • {f}")
        print(f"\n   Please run ETL_project.py first to generate these files.")
        return False

    return True


def load_csv(file_path):
    """Load CSV file into list of dictionaries"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return []
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return []


def safe_int(value):
    """Convert to integer, return None if invalid"""
    try:
        return int(value) if value and str(value).strip() else None
    except:
        return None


def safe_float(value):
    """Convert to float, return None if invalid"""
    try:
        return float(value) if value and str(value).strip() else None
    except:
        return None


def safe_date(value):
    """Convert to date, return None if invalid"""
    if not value or str(value).strip() == '':
        return None
    try:
        return datetime.strptime(str(value), '%Y-%m-%d').date()
    except:
        return None


def safe_bool(value):
    """Convert to boolean"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 't', 'yes')
    return bool(value)


# ==========================================
# DATABASE SETUP FUNCTIONS
# ==========================================

def get_connection():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        print(f"✓ Connected to PostgreSQL database '{DB_CONFIG['database']}'")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("\n⚠️  Please check:")
        print("   1. PostgreSQL is running")
        print("   2. Database credentials in DB_CONFIG are correct")
        print("   3. Database exists")
        return None


def create_schema_and_tables(conn):
    """Create schema and all tables"""
    cursor = conn.cursor()
    schema = DB_CONFIG['schema']

    try:
        print(f"\n[1] Creating schema and tables...")

        # Create schema
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        print(f"✓ Schema '{schema}' created")

        # Drop existing tables (fresh start)
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.fact_inventory CASCADE;")
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.fact_enrollment CASCADE;")
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_item CASCADE;")
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_supplier CASCADE;")
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_category CASCADE;")
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_student CASCADE;")
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.dim_major CASCADE;")

        # Create Sales Dimension Tables

        cursor.execute(f"""
            CREATE TABLE {schema}.dim_item (
                item_id INT PRIMARY KEY,
                item_name TEXT,
                category TEXT
            );
        """)
        print("✓ Created table: dim_item")

        cursor.execute(f"""
            CREATE TABLE {schema}.dim_supplier (
                supplier_id SERIAL PRIMARY KEY,
                supplier_name TEXT UNIQUE
            );
        """)
        print("✓ Created table: dim_supplier")

        cursor.execute(f"""
            CREATE TABLE {schema}.dim_category (
                category_id SERIAL PRIMARY KEY,
                category_name TEXT UNIQUE
            );
        """)
        print("✓ Created table: dim_category")

        # Create Sales Fact Table

        cursor.execute(f"""
            CREATE TABLE {schema}.fact_inventory (
                inventory_id SERIAL PRIMARY KEY,
                item_id INT,
                supplier_id INT,
                category_id INT,
                date_added DATE,
                quantity INT,
                price NUMERIC(10,2),
                total_value NUMERIC(10,2),
                has_valid_date BOOLEAN,
                has_valid_price BOOLEAN,
                FOREIGN KEY (item_id) REFERENCES {schema}.dim_item(item_id),
                FOREIGN KEY (supplier_id) REFERENCES {schema}.dim_supplier(supplier_id),
                FOREIGN KEY (category_id) REFERENCES {schema}.dim_category(category_id)
            );
        """)
        print("✓ Created table: fact_inventory")

        # Create Student Dimension Tables

        cursor.execute(f"""
            CREATE TABLE {schema}.dim_student (
                student_id INT PRIMARY KEY,
                name TEXT,
                age INT,
                gender TEXT
            );
        """)
        print("✓ Created table: dim_student")

        cursor.execute(f"""
            CREATE TABLE {schema}.dim_major (
                major_id SERIAL PRIMARY KEY,
                major_name TEXT UNIQUE
            );
        """)
        print("✓ Created table: dim_major")

        # Create Student Fact Table

        cursor.execute(f"""
            CREATE TABLE {schema}.fact_enrollment (
                enrollment_id SERIAL PRIMARY KEY,
                student_id INT,
                major_id INT,
                grade TEXT,
                enrollment_date DATE,
                days_enrolled INT,
                has_valid_age BOOLEAN,
                has_valid_enrollment_date BOOLEAN,
                FOREIGN KEY (student_id) REFERENCES {schema}.dim_student(student_id),
                FOREIGN KEY (major_id) REFERENCES {schema}.dim_major(major_id)
            );
        """)
        print("✓ Created table: fact_enrollment")

        conn.commit()
        print(f"✓ All tables created successfully in schema '{schema}'")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error creating tables: {e}")
        raise
    finally:
        cursor.close()


# ==========================================
# DATA LOADING FUNCTIONS
# ==========================================

def load_sales_data(conn, csv_file):
    """Load sales data into dimension and fact tables"""
    cursor = conn.cursor()
    schema = DB_CONFIG['schema']

    print(f"\n[2] Loading sales data from {csv_file}...")

    # Load CSV
    sales_data = load_csv(csv_file)
    if not sales_data:
        print("❌ No sales data to load")
        return

    try:
        # Track unique values for dimensions
        supplier_lookup = {}
        category_lookup = {}
        loaded_items = set()

        # Load dimension tables first
        print("   Loading dimension tables...")

        for row in sales_data:
            item_id = safe_int(row.get('item_id'))
            if not item_id:
                continue

            # DIM_ITEM (deduplicated)
            if item_id not in loaded_items:
                cursor.execute(f"""
                    INSERT INTO {schema}.dim_item (item_id, item_name, category)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (item_id) DO NOTHING
                """, (
                    item_id,
                    row.get('item_name'),
                    row.get('category')
                ))
                loaded_items.add(item_id)

            # DIM_SUPPLIER
            supplier = row.get('supplier')
            if supplier and supplier not in supplier_lookup:
                cursor.execute(f"""
                    INSERT INTO {schema}.dim_supplier (supplier_name)
                    VALUES (%s)
                    ON CONFLICT (supplier_name) DO NOTHING
                    RETURNING supplier_id
                """, (supplier,))
                result = cursor.fetchone()
                if result:
                    supplier_lookup[supplier] = result[0]
                else:
                    # If conflict, fetch existing ID
                    cursor.execute(f"""
                        SELECT supplier_id FROM {schema}.dim_supplier 
                        WHERE supplier_name = %s
                    """, (supplier,))
                    supplier_lookup[supplier] = cursor.fetchone()[0]

            # DIM_CATEGORY
            category = row.get('category')
            if category and category not in category_lookup:
                cursor.execute(f"""
                    INSERT INTO {schema}.dim_category (category_name)
                    VALUES (%s)
                    ON CONFLICT (category_name) DO NOTHING
                    RETURNING category_id
                """, (category,))
                result = cursor.fetchone()
                if result:
                    category_lookup[category] = result[0]
                else:
                    cursor.execute(f"""
                        SELECT category_id FROM {schema}.dim_category 
                        WHERE category_name = %s
                    """, (category,))
                    category_lookup[category] = cursor.fetchone()[0]

        print(f"   ✓ Loaded {len(loaded_items)} items")
        print(f"   ✓ Loaded {len(supplier_lookup)} suppliers")
        print(f"   ✓ Loaded {len(category_lookup)} categories")

        # Load fact table
        print("   Loading fact table...")
        fact_count = 0

        for row in sales_data:
            item_id = safe_int(row.get('item_id'))
            if not item_id:
                continue

            supplier_id = supplier_lookup.get(row.get('supplier'))
            category_id = category_lookup.get(row.get('category'))

            cursor.execute(f"""
                INSERT INTO {schema}.fact_inventory (
                    item_id, supplier_id, category_id, date_added,
                    quantity, price, total_value, has_valid_date, has_valid_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item_id,
                supplier_id,
                category_id,
                safe_date(row.get('date_added')),
                safe_int(row.get('quantity')),
                safe_float(row.get('price')),
                safe_float(row.get('total_value')),
                safe_bool(row.get('has_valid_date')),
                safe_bool(row.get('has_valid_price'))
            ))
            fact_count += 1

        conn.commit()
        print(f"   ✓ Loaded {fact_count} records into fact_inventory")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error loading sales data: {e}")
        raise
    finally:
        cursor.close()


def load_student_data(conn, csv_file):
    """Load student data into dimension and fact tables"""
    cursor = conn.cursor()
    schema = DB_CONFIG['schema']

    print(f"\n[3] Loading student data from {csv_file}...")

    # Load CSV
    student_data = load_csv(csv_file)
    if not student_data:
        print("❌ No student data to load")
        return

    try:
        # Track unique majors
        major_lookup = {}
        loaded_students = set()

        # Load dimension tables
        print("   Loading dimension tables...")

        for row in student_data:
            student_id = safe_int(row.get('student_id'))
            if not student_id:
                continue

            # DIM_STUDENT (deduplicated)
            if student_id not in loaded_students:
                cursor.execute(f"""
                    INSERT INTO {schema}.dim_student (student_id, name, age, gender)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (student_id) DO NOTHING
                """, (
                    student_id,
                    row.get('name'),
                    safe_int(row.get('age')),
                    row.get('gender')
                ))
                loaded_students.add(student_id)

            # DIM_MAJOR
            major = row.get('major')
            if major and major not in major_lookup:
                cursor.execute(f"""
                    INSERT INTO {schema}.dim_major (major_name)
                    VALUES (%s)
                    ON CONFLICT (major_name) DO NOTHING
                    RETURNING major_id
                """, (major,))
                result = cursor.fetchone()
                if result:
                    major_lookup[major] = result[0]
                else:
                    cursor.execute(f"""
                        SELECT major_id FROM {schema}.dim_major 
                        WHERE major_name = %s
                    """, (major,))
                    major_lookup[major] = cursor.fetchone()[0]

        print(f"   ✓ Loaded {len(loaded_students)} students")
        print(f"   ✓ Loaded {len(major_lookup)} majors")

        # Load fact table
        print("   Loading fact table...")
        fact_count = 0

        for row in student_data:
            student_id = safe_int(row.get('student_id'))
            if not student_id:
                continue

            major_id = major_lookup.get(row.get('major'))

            cursor.execute(f"""
                INSERT INTO {schema}.fact_enrollment (
                    student_id, major_id, grade, enrollment_date,
                    days_enrolled, has_valid_age, has_valid_enrollment_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                student_id,
                major_id,
                row.get('grade'),
                safe_date(row.get('enrollment_date')),
                safe_int(row.get('days_enrolled')),
                safe_bool(row.get('has_valid_age')),
                safe_bool(row.get('has_valid_enrollment_date'))
            ))
            fact_count += 1

        conn.commit()
        print(f"   ✓ Loaded {fact_count} records into fact_enrollment")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error loading student data: {e}")
        raise
    finally:
        cursor.close()


def verify_data(conn):
    """Verify data was loaded correctly"""
    cursor = conn.cursor()
    schema = DB_CONFIG['schema']

    print(f"\n[4] Verifying data...")

    tables = [
        'dim_item', 'dim_supplier', 'dim_category',
        'fact_inventory', 'dim_student', 'dim_major',
        'fact_enrollment'
    ]

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        count = cursor.fetchone()[0]
        print(f"   {table}: {count} records")

    cursor.close()


def print_connection_info():
    """Print connection information for Power BI"""
    print("\n" + "=" * 70)
    print("✅ DATA LOADED SUCCESSFULLY!")
    print("=" * 70)
    print("\n📊 POWER BI CONNECTION INFORMATION:")
    print("-" * 70)
    print(f"Server: {DB_CONFIG['host']}")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"Port: {DB_CONFIG['port']}")
    print(f"Schema: {DB_CONFIG['schema']}")
    print(f"User: {DB_CONFIG['user']}")
    print("-" * 70)
    print("\n📋 TABLES AVAILABLE FOR POWER BI:")
    print("   Sales/Inventory:")
    print("   • dataeng_project.dim_item")
    print("   • dataeng_project.dim_supplier")
    print("   • dataeng_project.dim_category")
    print("   • dataeng_project.fact_inventory")
    print("\n   Students:")
    print("   • dataeng_project.dim_student")
    print("   • dataeng_project.dim_major")
    print("   • dataeng_project.fact_enrollment")
    print("-" * 70)


# ==========================================
# MAIN EXECUTION
# ==========================================

def main():
    """Main execution function"""
    print("=" * 70)
    print("PostgreSQL Data Loader for Power BI")
    print("=" * 70)

    # Check if CSV files exist
    print(f"\n[0] Checking for CSV files in '{OUTPUT_DIR}/'...")
    if not check_csv_files():
        return

    print(f"✓ Found all required CSV files")
    print(f"   • {CSV_FILES['sales']}")
    print(f"   • {CSV_FILES['students']}")

    # Connect to database
    conn = get_connection()
    if not conn:
        return

    try:
        # Create schema and tables
        create_schema_and_tables(conn)

        # Load sales data
        load_sales_data(conn, CSV_FILES['sales'])

        # Load student data
        load_student_data(conn, CSV_FILES['students'])

        # Verify data
        verify_data(conn)

        # Print connection info
        print_connection_info()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nPlease check the error message above and try again.")
    finally:
        conn.close()
        print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()