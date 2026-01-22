# Comprehensive Data Cleaning Rules Documentation

## Overview
This document details all data quality issues identified and the cleaning rules applied to handle them.

---

## Sales/Inventory Data Cleaning Rules

### 1. ItemID Validation
**Issue:** Missing or invalid ItemIDs  
**Rule:** 
- Must be a positive integer
- If missing, null, or invalid → **Skip entire row**
- This is a critical field required for all records

**Example:**
```
Before: ItemID = ""
After: Row skipped (no valid identifier)
```

---

### 2. Quantity Handling
**Issues Identified:**
- Negative quantities (e.g., -5)
- Empty/null values
- "N/A" text values

**Rules:**
- Negative values → Convert to **0** (invalid inventory count)
- Empty/null/"N/A" → Convert to **0**
- Only accept non-negative integers

**Examples:**
```
Before: Quantity = -5
After: Quantity = 0

Before: Quantity = ""
After: Quantity = 0

Before: Quantity = "N/A"
After: Quantity = 0
```

**Business Logic:** Negative inventory doesn't make sense, so we default to zero and flag for review.

---

### 3. Price Validation
**Issues Identified:**
- Negative prices (e.g., -20.5)
- Empty/null values
- "N/A" text values

**Rules:**
- Negative values → Convert to **0.0** (invalid price)
- Empty/null/"N/A" → Convert to **0.0**
- Add quality flag: `has_valid_price = False` when price = 0

**Examples:**
```
Before: Price = -20.5
After: Price = 0.0, has_valid_price = False

Before: Price = ""
After: Price = 0.0, has_valid_price = False
```

**Business Logic:** Negative prices are data errors. Zero prices might be free items or missing data.

---

### 4. Date Validation (Critical!)
**Issues Identified:**
- Invalid dates with month > 12 (e.g., "2026-13-01")
- Missing dates (empty fields)
- Multiple date formats

**Rules:**
- Validate month is 1-12, day is 1-31
- Validate year is between 1900-2030
- Support multiple formats:
  - YYYY-MM-DD (2025-08-10)
  - MM/DD/YYYY (8/10/2025)
  - DD/MM/YYYY (10/08/2025)
- Invalid dates → **NULL** with flag `has_valid_date = False`

**Examples:**
```
Before: DateAdded = "2026-13-01" (month 13 invalid!)
After: DateAdded = NULL, has_valid_date = False

Before: DateAdded = "8/10/2025"
After: DateAdded = "2025-08-10", has_valid_date = True

Before: DateAdded = ""
After: DateAdded = NULL, has_valid_date = False
```

**Business Logic:** Invalid dates are rejected rather than guessed to prevent data corruption.

---

### 5. Supplier Standardization
**Issues Identified:**
- Empty/missing supplier names
- Inconsistent capitalization

**Rules:**
- Empty/null → Default to **"Unknown"**
- Standardize to Title Case
- Remove extra whitespace

**Examples:**
```
Before: Supplier = ""
After: Supplier = "Unknown"

Before: Supplier = "SupplierB"
After: Supplier = "Supplierb"
```

---

### 6. Category & Item Name Cleaning
**Rules:**
- Remove extra whitespace
- Standardize to Title Case
- Empty/null → NULL

**Examples:**
```
Before: ItemName = "  water  "
After: ItemName = "Water"

Before: Category = "ELECTRONICS"
After: Category = "Electronics"
```

---

### 7. Calculated Fields
**Total Value:**
```
total_value = quantity × price
```

**Examples:**
```
Quantity = 27, Price = 60.42
Total Value = 1,631.34

Quantity = 0, Price = 393.26
Total Value = 0.0 (no inventory!)
```

---

## Student Data Cleaning Rules

### 1. StudentID Validation
**Rule:** 
- Must be a positive integer
- If missing or invalid → **Skip entire row**

---

### 2. Age Validation (Critical!)
**Issues Identified:**
- Negative ages (e.g., -1)
- Word representations (e.g., "twenty")
- Out-of-range values

**Rules:**
- Negative values → **NULL** with flag `has_valid_age = False`
- Ages outside 1-120 range → **NULL**
- Convert word representations to numbers:
  - "twenty" → 20
  - "twenty one" → 21
  - "eighteen" → 18

**Examples:**
```
Before: Age = -1
After: Age = NULL, has_valid_age = False

Before: Age = "twenty"
After: Age = 20, has_valid_age = True

Before: Age = 150
After: Age = NULL, has_valid_age = False (unrealistic)
```

**Business Logic:** Negative ages are impossible. Ages over 120 are flagged as data errors.

---

### 3. Grade Standardization (Critical!)
**Issues Identified:**
- Grades beyond F scale (e.g., G, H, Z)
- Plus/minus variations (A+, B-)

**Rules:**
- Valid grades: A+, A, A-, B+, B, B-, C+, C, C-, D+, D, D-, F
- Any grade beyond F (G, H, I...Z) → **Convert to F**
- Maintain plus/minus modifiers for valid grades

**Examples:**
```
Before: Grade = "A+"
After: Grade = "A+" (valid, keep as-is)

Before: Grade = "Z"
After: Grade = "F" (beyond scale, failing)

Before: Grade = "G"
After: Grade = "F" (beyond scale, failing)

Before: Grade = "B"
After: Grade = "B" (valid)
```

**Business Logic:** Grades beyond F are data errors and represent failing performance.

---

### 4. Enrollment Date Validation
**Issues Identified:**
- Invalid dates (month 15: "2026-15-01")
- Missing dates
- Multiple formats

**Rules:**
- Same validation as sales dates
- Validate month 1-12, day 1-31, year 1900-2030
- Calculate `days_enrolled` if valid date exists
- Invalid → NULL with flag `has_valid_enrollment_date = False`

**Examples:**
```
Before: EnrollmentDate = "2026-15-01" (month 15!)
After: EnrollmentDate = NULL, has_valid_enrollment_date = False

Before: EnrollmentDate = "10/31/2021"
After: EnrollmentDate = "2021-10-31", days_enrolled = 1,540

Before: EnrollmentDate = ""
After: EnrollmentDate = NULL, days_enrolled = NULL
```

---

### 5. Gender Standardization
**Rules:**
- Standardize to M/F/Other
- Handle variations:
  - "M", "Male", "Man" → **M**
  - "F", "Female", "Woman" → **F**
  - Everything else → **Other**
- Empty/null → **NULL**

**Examples:**
```
Before: Gender = "Female"
After: Gender = "F"

Before: Gender = "M"
After: Gender = "M"

Before: Gender = ""
After: Gender = NULL
```

---

### 6. Name Cleaning
**Rules:**
- Remove extra whitespace
- Title Case standardization
- Handle split first/last names if needed

**Examples:**
```
Before: Name = "john clark"
After: Name = "John Clark"

Before: Name = "  TAMMY   RAMIREZ  "
After: Name = "Tammy Ramirez"
```

---

### 7. Major Standardization
**Rules:**
- Title Case
- Remove extra whitespace
- Empty/null → NULL

**Examples:**
```
Before: Major = "BIOLOGY"
After: Major = "Biology"

Before: Major = ""
After: Major = NULL
```

---

## Data Quality Flags

The cleaning process adds quality indicator flags to help analysts identify problematic records:

### Sales Data Flags:
- `has_valid_date`: True if DateAdded is valid
- `has_valid_price`: True if Price > 0

### Student Data Flags:
- `has_valid_age`: True if Age is in valid range
- `has_valid_enrollment_date`: True if EnrollmentDate is valid

---

## Sample Transformations from Your Data

### Sales Record Transformations:

**Row 1 (Invalid Date):**
```
BEFORE:
ItemID: 1, ItemName: Understand, Quantity: 12, Price: (empty), 
DateAdded: 2026-13-01, Supplier: SupplierB

AFTER:
item_id: 1, item_name: "Understand", quantity: 12, price: 0.0,
date_added: NULL, supplier: "Supplierb", total_value: 0.0,
has_valid_date: False, has_valid_price: False
```

**Row 2 (Negative Price):**
```
BEFORE:
ItemID: 2, Quantity: 21, Price: -20.5, DateAdded: 8/10/2025

AFTER:
item_id: 2, quantity: 21, price: 0.0, date_added: "2025-08-10",
total_value: 0.0, has_valid_date: True, has_valid_price: False
```

**Row 4 (Negative Quantity):**
```
BEFORE:
ItemID: 4, Quantity: -5, Price: (empty), DateAdded: (empty)

AFTER:
item_id: 4, quantity: 0, price: 0.0, date_added: NULL,
total_value: 0.0, has_valid_date: False, has_valid_price: False
```

**Row 6 (Valid Record):**
```
BEFORE:
ItemID: 6, Quantity: 27, Price: 60.42, Supplier: (empty)

AFTER:
item_id: 6, quantity: 27, price: 60.42, supplier: "Unknown",
total_value: 1631.34, has_valid_date: True, has_valid_price: True
```

---

### Student Record Transformations:

**Row 1 (Invalid Date, Word Age):**
```
BEFORE:
StudentID: 1, Name: John Clark, Age: twenty, Gender: Female,
Grade: A+, EnrollmentDate: 2026-15-01

AFTER:
student_id: 1, name: "John Clark", age: 20, gender: "F",
grade: "A+", enrollment_date: NULL, days_enrolled: NULL,
has_valid_age: True, has_valid_enrollment_date: False
```

**Row 3 (Negative Age):**
```
BEFORE:
StudentID: 3, Name: Tammy Ramirez, Age: -1, Grade: D

AFTER:
student_id: 3, name: "Tammy Ramirez", age: NULL, gender: "F",
grade: "D", has_valid_age: False
```

**Row 7 (Negative Age, Missing Gender):**
```
BEFORE:
StudentID: 7, Name: Daniel Williams, Age: -1, Gender: (empty)

AFTER:
student_id: 7, name: "Daniel Williams", age: NULL, gender: NULL,
grade: "B", has_valid_age: False
```

**Row 8 (Valid Record):**
```
BEFORE:
StudentID: 8, Name: Gwendolyn Davis, Age: twenty, 
EnrollmentDate: 4/11/2025

AFTER:
student_id: 8, name: "Gwendolyn Davis", age: 20, gender: "F",
enrollment_date: "2025-04-11", days_enrolled: 282,
has_valid_age: True, has_valid_enrollment_date: True
```

---

## Data Quality Summary

Based on your 9 sample sales records:
- **0 records** fully valid (all have issues)
- **1 record** with valid price (Row 6 only)
- **3 records** with valid dates
- **5 records** with invalid dates (month 13 or empty)
- **4 records** with negative quantities
- **3 records** with negative/missing prices

Based on your 9 sample student records:
- **2 records** fully valid (Rows 8, 9)
- **2 records** with negative ages (flagged)
- **7 records** with invalid enrollment dates
- **All grades** properly standardized

---

## Files Generated

After running the ETL pipeline, you'll get:

1. **cleaned_sales_data.csv** - All sales records with quality flags
2. **cleaned_student_data.csv** - All student records with quality flags
3. **dim_item.csv** - Item dimension table
4. **dim_supplier.csv** - Supplier dimension table
5. **dim_category.csv** - Category dimension table
6. **dim_student.csv** - Student dimension table
7. **dim_major.csv** - Major dimension table
8. **fact_inventory.csv** - Inventory fact table
9. **fact_enrollment.csv** - Enrollment fact table
10. **data_quality_report.json** - Quality metrics summary

---

## Recommendations for Analysts

1. **Filter out low-quality records** using the quality flags
2. **Investigate records** where `has_valid_date = False`
3. **Review items** with `price = 0` before analysis
4. **Check students** with missing ages or enrollment dates
5. **Use the fact tables** for aggregated analytics
6. **Reference dimension tables** for filtering and grouping

This ensures clean, trustworthy data for downstream analysis and dashboards!