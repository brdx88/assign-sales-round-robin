# ETL Round Robin Lead Assignment in Python

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
The script leverages `pandas` to perform ranking and merging operations to ensure each lead is assigned to a sales team member, while maintaining a balanced distribution across salespeople.

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
python = ....
```
For the full code, refer to the project file `round_robin_leads_sales.py`.

## Dependencies

## How to Run the Script

## Example Output
