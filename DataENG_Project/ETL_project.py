import csv
from datetime import datetime, date
import json
import os
import numpy as np

# PART 1: ENHANCED CLEANING FUNCTIONS

def safe_int(value, allow_negative=False):
    """
    Safely convert to integer with validation
    - Returns None for empty, "N/A", or invalid values
    - Optionally filters out negative numbers
    """
    if not value or str(value).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
        return None

    try:
        num = int(float(value))
        if not allow_negative and num < 0:
            return None  # Invalid: negative quantity/age
        return num
    except (ValueError, TypeError):
        return None


# def safe_float(value, allow_negative=False):
#     """
#     Safely convert to float with validation
#     - Handles negative prices (set to None)
#     - Returns None for invalid values
#     """
#     if not value or str(value).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
#         return None
#
#     try:
#         num = float(value)
#         if not allow_negative and num < 0:
#             return None  # Invalid: negative price
#         return num
#     except (ValueError, TypeError):
#         return None

def safe_float(value, allow_negative=False, strip_negative=False):
    """ Safely convert to float with validation
    Parameters:
    - allow_negative: allows true negative values
    - strip_negative: removes leading '-' if treated as data-entry mistake"""

    if not value or str(value).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
        return None

    try:
        val = str(value).strip()

        # ✅ Only strip '-' when explicitly allowed
        if strip_negative and val.startswith('-'):
            val = val[1:]

        num = float(val)

        if not allow_negative and num < 0:
            return None

        return num

    except (ValueError, TypeError):
        return None



def safe_date(date_str):
    """
    Parse date from multiple formats with validation
    - Validates month (1-12), day (1-31), year ranges
    - Returns None for invalid dates like 2026-13-01
    """
    if not date_str or str(date_str).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
        return None

    date_str = str(date_str).strip()

    # Try different date formats
    formats = [
        "%Y-%m-%d",  # 2025-01-15
        "%m/%d/%Y",  # 8/10/2025
        "%d/%m/%Y",  # 15/08/2025
        "%Y/%m/%d",  # 2025/08/10
        "%m-%d-%Y",  # 08-10-2025
        "%d-%m-%Y"  # 10-08-2025
    ]

    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt).date()

            # Validation checks
            if parsed_date.year < 1900 or parsed_date.year > 2030:
                continue  # Invalid year range

            if parsed_date.month < 1 or parsed_date.month > 12:
                continue  # Invalid month

            if parsed_date.day < 1 or parsed_date.day > 31:
                continue  # Invalid day

            # Valid date found
            return parsed_date

        except (ValueError, AttributeError):
            continue

    # No valid format found
    return None


def clean_text(value):
    """
    Clean and standardize text fields
    - Removes extra whitespace
    - Title case for names
    - Returns None for empty/invalid values
    """
    if not value or str(value).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
        return None

    # Remove extra spaces and title case
    cleaned = ' '.join(str(value).strip().split()).title()

    # Return None if resulting string is empty
    return cleaned if cleaned else None


def age_word_to_int(age):
    """
    Convert age words to integers with validation
    - Handles word representations
    - Filters out negative ages
    - Valid age range: 1-120
    """
    if not age or str(age).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
        return None

    # Word mapping
    word_mapping = {
        "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
        "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
        "twenty one": 21, "twenty two": 22, "twenty three": 23, "twenty four": 24,
        "twenty five": 25, "twenty six": 26, "twenty seven": 27, "twenty eight": 28,
        "twenty nine": 29, "thirty": 30
    }

    age_str = str(age).strip().lower()

    # Try word mapping first
    if age_str in word_mapping:
        return word_mapping[age_str]

    # Try numeric conversion
    try:
        age_num = int(float(age))
        # Validate reasonable age range
        if 1 <= age_num <= 120:
            return age_num
        else:
            return None  # Invalid age
    except (ValueError, TypeError):
        return None


def standardize_gender(gender):
    """
    Standardize gender values to M/F/Other
    - Handles various input formats
    - Returns None for empty/invalid values
    """
    if not gender or str(gender).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
        return None

    g = str(gender).strip().upper()

    if g in ['M', 'MALE', 'MAN', 'BOY']:
        return 'M'
    elif g in ['F', 'FEMALE', 'WOMAN', 'GIRL']:
        return 'F'
    else:
        return 'Other'


def standardize_grade(grade):
    """
    Standardize grade to A-F scale
    - Handles A+, A, A-, B+, etc.
    - Anything below F or invalid is converted to F
    - Returns None for empty values
    """
    if not grade or str(grade).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
        return None

    grade_str = str(grade).strip().upper()

    # Valid grades: A+, A, A-, B+, B, B-, C+, C, C-, D+, D, D-, F
    valid_base_grades = ['A', 'B', 'C', 'D', 'F']

    # Extract base grade (first letter)
    if grade_str:
        base_grade = grade_str[0]

        # If grade is beyond F (like G, H, Z) or invalid, convert to F
        if base_grade not in valid_base_grades:
            return 'F'

        # Return the original grade if valid
        return grade_str

    return None


def clean_supplier(supplier):
    """
    Clean supplier names
    - Returns None for empty values
    - Standardizes format
    """
    if not supplier or str(supplier).strip() in ['', 'N/A', 'n/a', 'NA', 'null', 'None']:
        return 'Unknown'  # Default supplier for missing values

    return clean_text(supplier)


def combine_name_parts(first, last):
    """Combine first and last name if split"""
    parts = []
    if first and str(first).strip():
        parts.append(str(first).strip().title())
    if last and str(last).strip():
        parts.append(str(last).strip().title())
    return ' '.join(parts) if parts else None


# PART 2: DATA TRANSFORMATION WITH VALIDATION


def transform_sales_data(rows):
    """
    Clean and transform sales/inventory data with comprehensive validation

    Quality Rules:
    1. ItemID must be valid positive integer (required)
    2. Quantity must be non-negative (negative = 0)
    3. Price must be non-negative (negative = None, skip row)
    4. DateAdded must be valid date (invalid dates = None)
    5. Supplier cannot be empty (default to 'Unknown')
    """
    cleaned = []
    skipped_count = 0

    for idx, r in enumerate(rows):
        # Skip completely empty rows
        if not any(r.values()):
            skipped_count += 1
            continue

        # Critical field: ItemID
        item_id = safe_int(r.get("ItemID"), allow_negative=False)
        if item_id is None:
            skipped_count += 1
            continue  # Skip row without valid ItemID

        # Clean quantity (negative becomes 0)
        quantity = safe_int(r.get("Quantity"), allow_negative=False)
        if quantity is None:
            quantity = 0

        # Clean price (must be non-negative)
        # price = safe_float(r.get("Price"), allow_negative=False)
        price = safe_float(r.get("Price"),allow_negative=False,strip_negative=True)  # ✅ instructor requirement

        if price is None:
            price = 0.0

        # Clean date
        date_added = safe_date(r.get("DateAdded"))

        # Clean supplier
        supplier = clean_supplier(r.get("Supplier"))

        cleaned_row = {"item_id": item_id, "item_name": clean_text(r.get("ItemName")),
                       "category": clean_text(r.get("Category")), "quantity": quantity, "price": price,
                       "supplier": supplier, "date_added": date_added}

        # Calculate total value
        cleaned_row["total_value"] = round(cleaned_row["quantity"] * cleaned_row["price"], 2)

        # Data quality flags
        cleaned_row["has_valid_date"] = date_added is not None
        cleaned_row["has_valid_price"] = price > 0

        cleaned.append(cleaned_row)

    print(f"   Sales: Processed {len(rows)} rows, cleaned {len(cleaned)} rows, skipped {skipped_count} rows")
    return cleaned


def transform_student_data(rows):
    """
    Clean and transform student data with comprehensive validation

    Quality Rules:
    1. StudentID must be valid positive integer (required)
    2. Age must be in range 1-120 (negative/invalid = None)
    3. Grade must be A-F (anything beyond F = F)
    4. EnrollmentDate must be valid (invalid = None)
    5. Gender must be M/F/Other
    """
    cleaned = []
    skipped_count = 0

    for idx, r in enumerate(rows):
        # Skip completely empty rows
        if not any(r.values()):
            skipped_count += 1
            continue

        # Critical field: StudentID
        student_id = safe_int(r.get("StudentID"), allow_negative=False)
        if student_id is None:
            skipped_count += 1
            continue  # Skip row without valid StudentID

        # Handle name (may be combined or split)
        name = clean_text(r.get("Name"))
        if not name and r.get("FirstName"):
            name = combine_name_parts(r.get("FirstName"), r.get("LastName"))

        # Clean age
        age = age_word_to_int(r.get("Age"))

        # Clean gender
        gender = standardize_gender(r.get("Gender"))

        # Clean grade
        grade = standardize_grade(r.get("Grade"))

        # Clean major
        major = clean_text(r.get("Major"))

        # Clean enrollment date
        enrollment_date = safe_date(r.get("EnrollmentDate"))

        cleaned_row = {
            "student_id": student_id,
            "name": name,
            "age": age,
            "gender": gender,
            "grade": grade,
            "major": major,
            "enrollment_date": enrollment_date
        }

        # Calculate days enrolled (if enrollment date exists)
        if enrollment_date:
            days_enrolled = (date.today() - enrollment_date).days
            cleaned_row["days_enrolled"] = days_enrolled
        else:
            cleaned_row["days_enrolled"] = None

        # Data quality flags
        cleaned_row["has_valid_age"] = age is not None
        cleaned_row["has_valid_enrollment_date"] = enrollment_date is not None

        cleaned.append(cleaned_row)

    print(f"   Students: Processed {len(rows)} rows, cleaned {len(cleaned)} rows, skipped {skipped_count} rows")
    return cleaned


# ==========================================
# PART 3 ADDITIONAL: NUMPY ANALYTICS MODULE  
# ==========================================

def numpy_sales_analytics(sales_data):
    """Perform advanced NumPy analytics on sales data"""

    print("\n🔢 NumPy Analytics - Sales Data")
    print("=" * 70)

    analytics = {}

    # Extract numerical arrays
    prices = np.array([row['price'] for row in sales_data if row['price'] > 0])
    quantities = np.array([row['quantity'] for row in sales_data if row['quantity'] > 0])
    total_values = np.array([row['total_value'] for row in sales_data if row['total_value'] > 0])

    # 1. STATISTICAL ANALYSIS
    if len(prices) > 0:
        analytics['price_statistics'] = {
            'mean': float(np.mean(prices)),
            'median': float(np.median(prices)),
            'std_dev': float(np.std(prices)),
            'min': float(np.min(prices)),
            'max': float(np.max(prices)),
            'percentile_25': float(np.percentile(prices, 25)),
            'percentile_75': float(np.percentile(prices, 75)),
            'variance': float(np.var(prices))
        }

        print(f"\n📊 Price Statistics:")
        print(f"   Mean: ${analytics['price_statistics']['mean']:.2f}")
        print(f"   Median: ${analytics['price_statistics']['median']:.2f}")
        print(f"   Std Dev: ${analytics['price_statistics']['std_dev']:.2f}")
        print(f"   Range: ${analytics['price_statistics']['min']:.2f} - ${analytics['price_statistics']['max']:.2f}")

    # 2. OUTLIER DETECTION
    if len(prices) > 0:
        mean_price = np.mean(prices)
        std_price = np.std(prices)

        # Items more than 2 standard deviations from mean
        outlier_threshold_high = mean_price + (2 * std_price)
        outlier_threshold_low = mean_price - (2 * std_price)

        price_outliers = np.where((prices > outlier_threshold_high) | (prices < outlier_threshold_low))[0]

        analytics['outliers'] = {
            'count': int(len(price_outliers)),
            'threshold_high': float(outlier_threshold_high),
            'threshold_low': float(outlier_threshold_low),
            'percentage': float(len(price_outliers) / len(prices) * 100)
        }

        print(f"\n🚨 Outlier Detection:")
        print(f"   Outliers Found: {analytics['outliers']['count']} ({analytics['outliers']['percentage']:.1f}%)")
        print(f"   Thresholds: ${outlier_threshold_low:.2f} - ${outlier_threshold_high:.2f}")

    # 3. INVENTORY ANALYSIS
    if len(quantities) > 0:
        analytics['inventory_statistics'] = {
            'total_items': int(np.sum(quantities)),
            'mean_quantity': float(np.mean(quantities)),
            'median_quantity': float(np.median(quantities)),
            'low_stock_count': int(np.sum(quantities < 10))  # Items with quantity < 10
        }

        print(f"\n📦 Inventory Analysis:")
        print(f"   Total Items in Stock: {analytics['inventory_statistics']['total_items']}")
        print(f"   Average Quantity: {analytics['inventory_statistics']['mean_quantity']:.1f}")
        print(f"   Low Stock Items: {analytics['inventory_statistics']['low_stock_count']}")

    # 4. VALUE ANALYSIS
    if len(total_values) > 0:
        analytics['value_statistics'] = {
            'total_value': float(np.sum(total_values)),
            'mean_value': float(np.mean(total_values)),
            'high_value_items': int(np.sum(total_values > np.percentile(total_values, 75)))
        }

        print(f"\n💰 Value Analysis:")
        print(f"   Total Inventory Value: ${analytics['value_statistics']['total_value']:,.2f}")
        print(f"   Average Item Value: ${analytics['value_statistics']['mean_value']:.2f}")
        print(f"   High-Value Items (>75th percentile): {analytics['value_statistics']['high_value_items']}")

    # 5. BINNING - Price Categories
    if len(prices) > 0:
        price_bins = [0, 50, 100, 200, np.inf]
        price_labels = ['Low', 'Medium', 'High', 'Premium']
        price_categories = np.digitize(prices, price_bins)

        analytics['price_distribution'] = {
            'low': int(np.sum(price_categories == 1)),
            'medium': int(np.sum(price_categories == 2)),
            'high': int(np.sum(price_categories == 3)),
            'premium': int(np.sum(price_categories == 4))
        }

        print(f"\n🏷️  Price Distribution:")
        print(f"   Low ($0-$50): {analytics['price_distribution']['low']}")
        print(f"   Medium ($50-$100): {analytics['price_distribution']['medium']}")
        print(f"   High ($100-$200): {analytics['price_distribution']['high']}")
        print(f"   Premium ($200+): {analytics['price_distribution']['premium']}")

    # 6. CORRELATION ANALYSIS
    if len(prices) > 0 and len(quantities) > 0 and len(prices) == len(quantities):
        correlation = np.corrcoef(prices, quantities)[0, 1]
        analytics['correlation'] = {
            'price_quantity': float(correlation)
        }

        print(f"\n🔗 Correlation Analysis:")
        print(f"   Price-Quantity Correlation: {correlation:.3f}")
        if abs(correlation) < 0.3:
            print(f"   → Weak correlation")
        elif abs(correlation) < 0.7:
            print(f"   → Moderate correlation")
        else:
            print(f"   → Strong correlation")

    return analytics


def numpy_student_analytics(student_data):
    """Perform advanced NumPy analytics on student data"""

    print("\n\n🔢 NumPy Analytics - Student Data")
    print("=" * 70)

    analytics = {}

    # Extract numerical arrays
    ages = np.array([row['age'] for row in student_data if row['age'] is not None])
    days_enrolled = np.array([row['days_enrolled'] for row in student_data if row['days_enrolled'] is not None])

    # 1. AGE STATISTICS
    if len(ages) > 0:
        analytics['age_statistics'] = {
            'mean': float(np.mean(ages)),
            'median': float(np.median(ages)),
            'std_dev': float(np.std(ages)),
            'min': int(np.min(ages)),
            'max': int(np.max(ages)),
            'percentile_25': float(np.percentile(ages, 25)),
            'percentile_75': float(np.percentile(ages, 75))
        }

        print(f"\n📊 Age Statistics:")
        print(f"   Mean Age: {analytics['age_statistics']['mean']:.1f}")
        print(f"   Median Age: {analytics['age_statistics']['median']:.1f}")
        print(f"   Age Range: {analytics['age_statistics']['min']} - {analytics['age_statistics']['max']}")
        print(f"   Std Dev: {analytics['age_statistics']['std_dev']:.2f}")

    # 2. AGE DISTRIBUTION (HISTOGRAM)
    if len(ages) > 0:
        age_bins = [0, 20, 25, 30, 35, 120]
        age_hist, _ = np.histogram(ages, bins=age_bins)

        analytics['age_distribution'] = {
            'under_20': int(age_hist[0]),
            '20_to_25': int(age_hist[1]),
            '25_to_30': int(age_hist[2]),
            '30_to_35': int(age_hist[3]),
            'over_35': int(age_hist[4])
        }

        print(f"\n👥 Age Distribution:")
        print(f"   Under 20: {analytics['age_distribution']['under_20']}")
        print(f"   20-25: {analytics['age_distribution']['20_to_25']}")
        print(f"   25-30: {analytics['age_distribution']['25_to_30']}")
        print(f"   30-35: {analytics['age_distribution']['30_to_35']}")
        print(f"   Over 35: {analytics['age_distribution']['over_35']}")

    # 3. OUTLIER DETECTION (Ages)
    if len(ages) > 0:
        mean_age = np.mean(ages)
        std_age = np.std(ages)

        age_outliers = np.where((ages > mean_age + 2 * std_age) | (ages < mean_age - 2 * std_age))[0]

        analytics['age_outliers'] = {
            'count': int(len(age_outliers)),
            'percentage': float(len(age_outliers) / len(ages) * 100)
        }

        print(f"\n🚨 Age Outliers:")
        print(f"   Unusual Ages: {analytics['age_outliers']['count']} ({analytics['age_outliers']['percentage']:.1f}%)")

    # 4. ENROLLMENT ANALYSIS
    if len(days_enrolled) > 0:
        analytics['enrollment_statistics'] = {
            'mean_days': float(np.mean(days_enrolled)),
            'median_days': float(np.median(days_enrolled)),
            'min_days': int(np.min(days_enrolled)),
            'max_days': int(np.max(days_enrolled)),
            'recently_enrolled': int(np.sum(days_enrolled < 365))  # Less than 1 year
        }

        print(f"\n📅 Enrollment Analysis:")
        print(f"   Average Days Enrolled: {analytics['enrollment_statistics']['mean_days']:.0f}")
        print(f"   Median Days Enrolled: {analytics['enrollment_statistics']['median_days']:.0f}")
        print(f"   Recently Enrolled (<1 year): {analytics['enrollment_statistics']['recently_enrolled']}")

    # 5. GRADE ANALYSIS
    grades = [row['grade'] for row in student_data if row['grade'] is not None]
    if grades:
        grade_mapping = {'A+': 4.3, 'A': 4.0, 'A-': 3.7, 'B+': 3.3, 'B': 3.0,
                         'B-': 2.7, 'C+': 2.3, 'C': 2.0, 'C-': 1.7, 'D': 1.0, 'F': 0.0}

        gpa_values = np.array([grade_mapping.get(g, 0.0) for g in grades])

        analytics['grade_statistics'] = {
            'mean_gpa': float(np.mean(gpa_values)),
            'median_gpa': float(np.median(gpa_values)),
            'failing_count': int(np.sum(gpa_values == 0.0))
        }

        print(f"\n📝 Grade Analysis:")
        print(f"   Average GPA: {analytics['grade_statistics']['mean_gpa']:.2f}")
        print(f"   Median GPA: {analytics['grade_statistics']['median_gpa']:.2f}")
        print(f"   Failing Students: {analytics['grade_statistics']['failing_count']}")

    # 6. GENDER DISTRIBUTION
    genders = [row['gender'] for row in student_data if row['gender'] is not None]
    if genders:
        unique_genders, gender_counts = np.unique(genders, return_counts=True)

        analytics['gender_distribution'] = {
            gender: int(count) for gender, count in zip(unique_genders, gender_counts)
        }

        print(f"\n⚥ Gender Distribution:")
        for gender, count in zip(unique_genders, gender_counts):
            percentage = (count / len(genders)) * 100
            print(f"   {gender}: {count} ({percentage:.1f}%)")

    return analytics


def calculate_data_quality_score(sales_data, student_data):
    """Calculate overall data quality score using NumPy"""

    print("\n\n Data Quality Score Calculation")
    print("=" * 70)

    # Sales data quality metrics
    total_sales = len(sales_data)
    valid_prices = np.sum([1 for row in sales_data if row['has_valid_price']])
    valid_dates = np.sum([1 for row in sales_data if row['has_valid_date']])

    sales_quality = (valid_prices / total_sales + valid_dates / total_sales) / 2 * 100

    # Student data quality metrics
    total_students = len(student_data)
    valid_ages = np.sum([1 for row in student_data if row['has_valid_age']])
    valid_enrollments = np.sum([1 for row in student_data if row['has_valid_enrollment_date']])

    student_quality = (valid_ages / total_students + valid_enrollments / total_students) / 2 * 100

    # Overall quality score
    overall_quality = (sales_quality + student_quality) / 2

    scores = {
        'sales_quality_score': float(sales_quality),
        'student_quality_score': float(student_quality),
        'overall_quality_score': float(overall_quality)
    }

    print(f"\n✅ Quality Scores:")
    print(f"   Sales Data Quality: {sales_quality:.1f}%")
    print(f"   Student Data Quality: {student_quality:.1f}%")
    print(f"   Overall Data Quality: {overall_quality:.1f}%")

    if overall_quality >= 90:
        print(f"   Rating: Excellent ⭐⭐⭐⭐⭐")
    elif overall_quality >= 75:
        print(f"   Rating: Good ⭐⭐⭐⭐")
    elif overall_quality >= 60:
        print(f"   Rating: Fair ⭐⭐⭐")
    else:
        print(f"   Rating: Needs Improvement ⭐⭐")

    return scores



# PART 3: DATA LOADING

def load_csv(file_path):
    """Load CSV file and return list of dictionaries"""
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return list(reader)
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        return []
    except Exception as e:
        print(f"Error loading {file_path}: {str(e)}")
        return []


# ==========================================
# PART 4: DIMENSIONAL MODEL CREATION
# ==========================================

def create_dimensional_tables(sales_data, student_data):
    """Create star schema with fact and dimension tables"""

    # ===== SALES DIMENSIONS =====

    # DIM_ITEM
    dim_item = []
    seen_items = set()
    for row in sales_data:
        if row['item_id'] and row['item_id'] not in seen_items:
            dim_item.append({
                'item_id': row['item_id'],
                'item_name': row['item_name'],
                'category': row['category']
            })
            seen_items.add(row['item_id'])

    # DIM_SUPPLIER
    dim_supplier = []
    seen_suppliers = {}
    supplier_id = 1
    for row in sales_data:
        if row['supplier'] and row['supplier'] not in seen_suppliers:
            seen_suppliers[row['supplier']] = supplier_id
            dim_supplier.append({
                'supplier_id': supplier_id,
                'supplier_name': row['supplier']
            })
            supplier_id += 1

    # DIM_CATEGORY
    dim_category = []
    seen_categories = {}
    category_id = 1
    for row in sales_data:
        if row['category'] and row['category'] not in seen_categories:
            seen_categories[row['category']] = category_id
            dim_category.append({
                'category_id': category_id,
                'category_name': row['category']
            })
            category_id += 1

    # FACT_INVENTORY
    fact_inventory = []
    for row in sales_data:
        fact_inventory.append({
            'item_id': row['item_id'],
            'supplier_id': seen_suppliers.get(row['supplier']),
            'category_id': seen_categories.get(row['category']),
            'date_added': row['date_added'],
            'quantity': row['quantity'],
            'price': row['price'],
            'total_value': row['total_value'],
            'has_valid_date': row['has_valid_date'],
            'has_valid_price': row['has_valid_price']
        })

    # ===== STUDENT DIMENSIONS =====

    # DIM_STUDENT
    dim_student = []
    for row in student_data:
        dim_student.append({
            'student_id': row['student_id'],
            'name': row['name'],
            'age': row['age'],
            'gender': row['gender']
        })

    # DIM_MAJOR
    dim_major = []
    seen_majors = {}
    major_id = 1
    for row in student_data:
        if row['major'] and row['major'] not in seen_majors:
            seen_majors[row['major']] = major_id
            dim_major.append({
                'major_id': major_id,
                'major_name': row['major']
            })
            major_id += 1

    # FACT_ENROLLMENT
    fact_enrollment = []
    for row in student_data:
        fact_enrollment.append({
            'student_id': row['student_id'],
            'major_id': seen_majors.get(row['major']),
            'grade': row['grade'],
            'enrollment_date': row['enrollment_date'],
            'days_enrolled': row['days_enrolled'],
            'has_valid_age': row['has_valid_age'],
            'has_valid_enrollment_date': row['has_valid_enrollment_date']
        })

    return {
        'sales_dimensions': {
            'dim_item': dim_item,
            'dim_supplier': dim_supplier,
            'dim_category': dim_category,
            'fact_inventory': fact_inventory
        },
        'student_dimensions': {
            'dim_student': dim_student,
            'dim_major': dim_major,
            'fact_enrollment': fact_enrollment
        }
    }


# ==========================================
# PART 5: OUTPUT DIRECTORY & DATA EXPORT
# ==========================================

def create_output_directory():
    """Create output_files directory if it doesn't exist"""
    output_dir = 'output_files'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"✓ Created directory: {output_dir}")
    return output_dir


def export_to_csv(data, filename, fieldnames, output_dir='output_files'):
    """Export cleaned data to CSV in output_files directory"""
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Exported {len(data)} records to {filepath}")


def generate_data_quality_report(sales_data, student_data):
    """Generate data quality summary report"""

    report = {
        "sales_quality": {
            "total_records": len(sales_data),
            "missing_dates": sum(1 for r in sales_data if not r['has_valid_date']),
            "zero_prices": sum(1 for r in sales_data if r['price'] == 0),
            "zero_quantity": sum(1 for r in sales_data if r['quantity'] == 0),
            "avg_total_value": sum(r['total_value'] for r in sales_data) / len(sales_data) if sales_data else 0
        },
        "student_quality": {
            "total_records": len(student_data),
            "missing_ages": sum(1 for r in student_data if not r['has_valid_age']),
            "missing_enrollment_dates": sum(1 for r in student_data if not r['has_valid_enrollment_date']),
            "missing_majors": sum(1 for r in student_data if r['major'] is None),
            "avg_age": sum(r['age'] for r in student_data if r['age']) / sum(
                1 for r in student_data if r['age']) if any(r['age'] for r in student_data) else 0
        }
    }

    return report


# ==========================================
# MAIN EXECUTION
# ==========================================

def main():
    print("=" * 70)
    print("COMPREHENSIVE ETL PIPELINE WITH DATA QUALITY VALIDATION")
    print("=" * 70)

    # File paths
    # sales_file = "C:\\Users\\ashri\\Downloads\\sales_inventory_dataset.csv"
    sales_file = "C:\\Users\\ashri\\PyCharmMiscProject\\DataENG_Project\\sales_inventory_dataset.csv"
    # student_file = "C:\\Users\\ashri\\Downloads\\student_information_dataset.csv"
    student_file = "C:\\Users\\ashri\\PyCharmMiscProject\\DataENG_Project\\student_information_dataset.csv"

    # Create output directory
    print("\n[0] Setting up output directory...")
    output_dir = create_output_directory()

    # Load raw data
    print("\n[1] Loading raw data...")
    sales_data = load_csv(sales_file)
    student_data = load_csv(student_file)
    print(f"   Loaded {len(sales_data)} sales records")
    print(f"   Loaded {len(student_data)} student records")

    if not sales_data or not student_data:
        print("\n❌ Error: Could not load data files. Please check file paths.")
        return

    # Transform data
    print("\n[2] Cleaning and transforming data...")
    clean_sales = transform_sales_data(sales_data)
    clean_students = transform_student_data(student_data)

    # ==============================
    # NumPy Analytics
    # ==============================
    print("\n[3] Running NumPy analytics on cleaned data...")
    sales_numpy_analytics = numpy_sales_analytics(clean_sales)
    student_numpy_analytics = numpy_student_analytics(clean_students)

    print("\n[4] Calculating overall data quality score...")
    quality_scores = calculate_data_quality_score(clean_sales, clean_students)

    # Create dimensional model
    print("\n[3] Building dimensional model (Star Schema)...")
    dimensions = create_dimensional_tables(clean_sales, clean_students)
    print(f"   Created sales star schema with {len(dimensions['sales_dimensions'])} tables")
    print(f"   Created student star schema with {len(dimensions['student_dimensions'])} tables")

    # Generate quality report
    print("\n[4] Generating data quality report...")
    quality_report = generate_data_quality_report(clean_sales, clean_students)

    # Export cleaned data
    print(f"\n[5] Exporting cleaned data to {output_dir}/...")

    # Export cleaned raw data
    export_to_csv(
        clean_sales,
        'cleaned_sales_data.csv',
        ['item_id', 'item_name', 'category', 'quantity', 'price', 'supplier',
         'date_added', 'total_value', 'has_valid_date', 'has_valid_price'],
        output_dir
    )

    export_to_csv(
        clean_students,
        'cleaned_student_data.csv',
        ['student_id', 'name', 'age', 'gender', 'grade', 'major',
         'enrollment_date', 'days_enrolled', 'has_valid_age', 'has_valid_enrollment_date'],
        output_dir
    )

    # Export dimensional tables
    export_to_csv(dimensions['sales_dimensions']['dim_item'], 'dim_item.csv',
                  ['item_id', 'item_name', 'category'], output_dir)
    export_to_csv(dimensions['sales_dimensions']['dim_supplier'], 'dim_supplier.csv',
                  ['supplier_id', 'supplier_name'], output_dir)
    export_to_csv(dimensions['sales_dimensions']['dim_category'], 'dim_category.csv',
                  ['category_id', 'category_name'], output_dir)
    export_to_csv(dimensions['sales_dimensions']['fact_inventory'], 'fact_inventory.csv',
                  ['item_id', 'supplier_id', 'category_id', 'date_added', 'quantity',
                   'price', 'total_value', 'has_valid_date', 'has_valid_price'], output_dir)

    export_to_csv(dimensions['student_dimensions']['dim_student'], 'dim_student.csv',
                  ['student_id', 'name', 'age', 'gender'], output_dir)
    export_to_csv(dimensions['student_dimensions']['dim_major'], 'dim_major.csv',
                  ['major_id', 'major_name'], output_dir)
    export_to_csv(dimensions['student_dimensions']['fact_enrollment'], 'fact_enrollment.csv',
                  ['student_id', 'major_id', 'grade', 'enrollment_date', 'days_enrolled',
                   'has_valid_age', 'has_valid_enrollment_date'], output_dir)

    # Export quality report to output directory
    report_path = os.path.join(output_dir, 'data_quality_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(quality_report, f, indent=2, default=str)
    print(f"✓ Exported data quality report to {report_path}")

    print("\n" + "=" * 70)
    print("DATA QUALITY SUMMARY")
    print("=" * 70)
    print(f"\nSALES DATA:")
    print(f"  Total Records: {quality_report['sales_quality']['total_records']}")
    print(f"  Missing/Invalid Dates: {quality_report['sales_quality']['missing_dates']}")
    print(f"  Zero/Invalid Prices: {quality_report['sales_quality']['zero_prices']}")
    print(f"  Zero Quantities: {quality_report['sales_quality']['zero_quantity']}")
    print(f"  Avg Total Value: ${quality_report['sales_quality']['avg_total_value']:.2f}")

    print(f"\nSTUDENT DATA:")
    print(f"  Total Records: {quality_report['student_quality']['total_records']}")
    print(f"  Missing/Invalid Ages: {quality_report['student_quality']['missing_ages']}")
    print(f"  Missing Enrollment Dates: {quality_report['student_quality']['missing_enrollment_dates']}")
    print(f"  Missing Majors: {quality_report['student_quality']['missing_majors']}")
    print(f"  Average Age: {quality_report['student_quality']['avg_age']:.1f}")

    print("\n" + "=" * 70)
    print("✓ ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"✓ All output files saved to: {os.path.abspath(output_dir)}")
    print("=" * 70)

    # Sample output
    if clean_sales:
        print("\n[SAMPLE CLEANED SALES RECORD]")
        print(clean_sales[0])

    if clean_students:
        print("\n[SAMPLE CLEANED STUDENT RECORD]")
        print(clean_students[0])


if __name__ == "__main__":
    main()