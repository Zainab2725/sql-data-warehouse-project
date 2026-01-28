# Naming Conventions

This document defines the naming rules for schemas, tables, views, columns, and other objects in the data warehouse.

---

## Table of Contents

1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   - [Bronze Rules](#bronze-rules)
   - [Silver Rules](#silver-rules)
   - [Gold Rules](#gold-rules)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
4. [Stored Procedure Naming](#stored-procedure-naming)

---

## General Principles

- **Format:** Use `snake_case` (lowercase letters, underscores between words).  
- **Language:** Always use English.  
- **Reserved Words:** Avoid using SQL reserved words.  

---

## Table Naming Conventions

### Bronze Rules
- Table names start with the source system name and match the source table exactly.  
- **Pattern:** `<sourcesystem>_<entity>`  
  - Example: `crm_customer_info` → Customer info from the CRM system.

### Silver Rules
- Same as Bronze: start with the source system name and match the source table.  
- **Pattern:** `<sourcesystem>_<entity>`  
  - Example: `erp_sales_order` → Sales order from ERP.

### Gold Rules
- Use meaningful, business-aligned names with a category prefix.  
- **Pattern:** `<category>_<entity>`  
  - `<category>`: Table type (`dim` for dimension, `fact` for fact).  
  - `<entity>`: Descriptive business name.  
- **Examples:**  
  - `dim_customers` → Dimension table for customers.  
  - `fact_sales` → Fact table for sales transactions.

#### Category Glossary

| Prefix      | Meaning         | Example(s)                        |
|------------|-----------------|----------------------------------|
| `dim_`     | Dimension table  | `dim_customer`, `dim_product`     |
| `fact_`    | Fact table       | `fact_sales`                       |
| `report_`  | Report table     | `report_customers`, `report_sales_monthly` |

---

## Column Naming Conventions

### Surrogate Keys
- Primary keys in dimension tables end with `_key`.  
- **Pattern:** `<table_name>_key`  
- **Example:** `customer_key` → Surrogate key in `dim_customers`.

### Technical Columns
- System columns start with `dwh_`.  
- **Pattern:** `dwh_<column_name>`  
- **Example:** `dwh_load_date` → Date when record was loaded.

---

## Stored Procedure Naming
- Follow the pattern: `load_<layer>`  
  - `<layer>`: Layer being loaded (`bronze`, `silver`, `gold`).  
- **Examples:**  
  - `load_bronze` → Loads Bronze layer.  
  - `load_silver` → Loads Silver layer.  
  - `load_gold` → Loads Gold layer.
