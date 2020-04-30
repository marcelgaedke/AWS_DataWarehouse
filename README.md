# Objective: Building a Data Warehouse based on JSON Files containing Log- and Song Data for the Sparkify Music Streaming App.

## Data Model
### The columns of the staging tables are based on the keys of the JSON Files allowing for easy loading. The Data from the staging tables 
### is then loaded into the star schema consisting of one central facts table 'songplay' with foreign keys to each of the four dimension tables 
### 'song', 'artist', 'user_table' and 'time'.

## Steps
### 1. Create AWS Infrastructure including IAM Roles, S3 and Redshift
### 2. Extract Data from JSON Files and load into staging tables on Redshift
### 3. Create Tables for Star Schema and load tables form staging tables
### 4. Run analytical queries based on final tables

## Files
### dwh.cfg - Config File containing static Data such as Constants, Paths, ARNs
### aws_setup.ipyb - Jupyter Notebook containing AWS setup (Infrastructure as Code). Using Boto3 to connect to AWS, create IAM Role and create Redshift Cluster
### create_tables.py - This file drops all tables and creates new tables
### etl.py - This file contains 3 functions calls: Load staging tables, Insert Statement to load star schema and Printing a summary of the rowcount of the resulting tables
### sql_queries.py - This file is imported from the other files and contains all the used sql queries