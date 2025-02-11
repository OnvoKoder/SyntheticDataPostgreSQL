# Synthetic Data PostgreSQL
This project generate synthetic data for PostgreSQL loading. If you use PostgreSQL database then this script create fake data for any table.
❗Only the table you are pointing to must exist. 

### Third-party library:
- `psycopg2` - library for connect to PostgreSQL
- `Faker` - library for generate fake data

### You need to run this command to load the libraries
```bash
pip install psycopg2 & pip install Faker
```
### This sql script to get names and data types for columns
```sql
SELECT column_name,
       data_type
  FROM information_schema.columns
 WHERE table_name = 'your table'
   AND table_schema = 'your schema'
```
