# ETL Round Robin Lead Assignment in PySpark

## Problem Statement
Managing the distribution of leads to the sales team manually can be both time-consuming and inefficient. In many cases, particularly in situations where the sales are organized by regions, branches, and sub-branches, you may encounter unassigned leads or leads assigned unevenly across salespeople. Manually addressing this, especially at scale, is prone to errors and introduces the need for constant supervision.

Imagine having to assign a large number of leads by clicking through interfaces or recalculating manually—an error-prone, labor-intensive process. This project automates lead assignment, ensuring fairness through a round-robin process, and dramatically reduces the need for constant manual intervention.

## Objective
The goal of this project is to automate the lead assignment process by:
1) Assigning leads to sales staff based on region, branch, and sub-branch levels.
1) Distributing unassigned leads to salespeople while ensuring that the distribution is balanced.
1) Handling cases where there is no exact match at the sub-branch level, and re-assigning leads at the branch level.
1) Summarizing the results to give insights into how leads were distributed across salespeople.

## Solution
This project implements a Python script that performs a round-robin distribution of leads across sales teams. The assignment operates at three levels:
1) **Region-Branch-Subbranch Level**: Primary assignment at the most granular level.
1) **Region-Branch Level**: Handling cases where there is no matching sub-branch.
1) **Fallback Assignment**: Re-distributing unassigned leads if necessary.
The script leverages `pyspark` to perform ranking and merging operations to ensure each lead is assigned to a sales team member, while maintaining a balanced distribution across salespeople.

## Workflow
1) Dataset Preparation:
    - `leads_dataset`: Contains leads with corresponding regional and branch details.
    - `sales_dataset`: Contains salespeople assigned to regions, branches, and sub-branches.

1) Round Robin Algorithm:
    - The sales dataset is ranked by region, branch, and sub-branch.
    - Leads are distributed in a balanced way using a modulo operation to ensure fair distribution.
    - If no sub-branch salespeople are available, leads are assigned to the branch level.

1) Unassigned Leads:
    - Any leads that cannot be assigned directly are handled separately, either by resetting their sub-branch values or by reassigning them at the branch level.

1) Final Results:
    - The script produces a summary showing how many leads were assigned directly and how many required fallback assignments.
    - The final merged dataset includes both assigned and unassigned leads.
  

## Key Features
- **Automated Lead Distribution**: Leads are distributed in a balanced, fair manner across all available salespeople.
- **Hierarchical Assignment**: Lead distribution is performed at the sub-branch level first, with fallback to branch level if necessary.
- **Fallback Handling**: Unassigned leads are handled gracefully, ensuring that no leads are left unassigned.
- **Easy-to-Understand Summary**: A clear summary of the distribution process and final assignments is provided for reporting purposes.

## Code Example
```python
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("ETLRoundRobinLeadsSales").getOrCreate()

# assume you have leads and sales dataset with region, branch, and subbranch code columns.
leads_df = spark.createDataFrame(leads_data, schema=leads_schema)
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Enhance Leads with sales id based on region-branch-subbranch
sales_assigned_df = round_robin_leads_sales(leads_df, sales_df)
sales_assigned_df
```
For the full code, refer to the project file `round_robin_leads_sales.py`.

## Dependencies
- `pyspark`: For handling the big data and applying transformations
- Python 3.x

Install the required libraries via pip:
    
```bash
pip install pyspark
```

## How to Run the Script
1) Prepare the datasets for leads and sales teams.
2) Run the Python script using the following command:
```bash
python etl_assign_sales_round_robin.py
```
The script will print the summary of lead assignment and return the final merged dataset with assigned and unassigned leads.

## Example Output
```
The result of Round Robin:
- Assigned directly = 500 leads
- Assigned with modify the subbranch into 0 value = 100 leads
- Assigned with branch-branch level = 50 leads
- Not Assigned yet = 5 leads

Total leads for checking purposes = 655 leads
```
