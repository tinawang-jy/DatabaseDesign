# NoSQL and Relational Database Design

## Overview
SuperDB is an interactive command-line interface (CLI) database management system. It allows users to perform a variety of database operations fro both NoSQL database and Relational database through a simple and intuitive text interface, similar to systems like MySQL or MongoDB.

## Features
- **Database Operations**: create, use, show, and drop databases.
- **Table Management**: create, import, show, and drop tables.
- **Data Manipulation**: insert, update, delete rows and columns.
- **Query Processing**: Support for complex queries including projection, filtering, joining, aggregation, group by and ordering.

## Example Queries
#### Database Operations
- Create a database: `CREATE DATABASE airbnb <NoSQL|Relational>;`
- Use a database: `USE DATABASE airbnb <NoSQL|Relational>;`
- Show all databases: `SHOW DATABASE;`
- Drop a database: `DROP DATABASE airbnb <NoSQL|Relational>;`

#### Table Operations
- Import a table: `IMPORT TABLE host FROM demoData/airbnb/host_demo.csv SET (host_id PRIMARY KEY);`
- Import another table: `IMPORT TABLE basic FROM demoData/airbnb/basic_demo.csv SET (id PRIMARY KEY, host_id FOREIGN KEY REF host$host_id);`
- Show all tables: `SHOW TABLES;`

#### Query Operations
- Find specific columns in a table: `FIND (id, name) IN TABLE basic;`
- Filter records: `FIND (id, name) IF host_id = “278008181” IN TABLE basic;`
- Join tables: `FIND (host$host_name, basic$name) JOIN BY host$host_id = basic$host_id ON <inner|left|right|outer>;`
- Group and aggregate: `FIND <COUNT|SUM|MEAN|MIN|MAX>(minimum_nights) GROUP BY price IN TABLE basic;`
- Order results: `FIND (id, name) IN TABLE basic ORDER BY minimum_nights <ASE|DESC>;`

### Modifications
- Use a database: `USE DATABASE airbnb NoSQL;`
- Create a table: `CREATE TABLE sample SET (ID PRIMARY KEY, name, host_id FOREIGN KEY REF host$host_id, price);`
- Insert data: `INSERT INTO sample(ID, name, host_id, price) VALUES (“001”, “Lovely Home”, 278008181, “500”);`
- Delete records: `DELETE FROM sample IF price < “700”;`
- Update records: `UPDATE sample SET (price = “over 1000”) IF price > “1000”;`
- Delete columns: `DELETE (name, host) FROM sample;`
- Drop a table: `DROP TABLE sample;`

## Installation
To get started with SuperDB, clone this repository to your local machine and run command line below in terminal with proper working directory
```bash
python3 SuperDB-CLI.py
