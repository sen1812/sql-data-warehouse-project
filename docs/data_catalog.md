Data Dictionary for Gold Layer
Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of dimension tables and fact tables for specific business metrics.

1. gold.dim_customers
Purpose:
Stores customer details enriched with demographic and geographic data.
Columns













































Column NameData TypeDescriptioncustomer_keyINTSurrogate key uniquely identifying each customer record in the dimension table.customer_idINTUnique numerical identifier assigned to each customer.customer_numberNVARCHAR(50)Alphanumeric identifier representing the customer, used for tracking and referencing.first_nameNVARCHAR(50)The customer's first name, as recorded in the system.last_nameNVARCHAR(50)The customer's last name or family name.countryNVARCHAR(50)The country of residence for the customer (e.g., 'Australia').marital_statusNVARCHAR(50)The marital status of the customer (e.g., 'Married', 'Single').
