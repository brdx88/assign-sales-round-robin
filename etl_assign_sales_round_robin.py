import pandas as pd

def round_robin_leads_sales(leads_dataset, sales_dataset):

    # Define copy dataset for leads and sales
    df_specific_sales_copy = sales_dataset.copy()
    union_no_npp_copy = leads_dataset.copy()

    # Rank the sales per subbranch_code
    df_specific_sales_copy['rn'] = df_specific_sales_copy.groupby(['region_code_sales','branch_code_sales','subbranch_code_sales']).cumcount() + 1
    # Rank the prospects per subbranch_code
    union_no_npp_copy['rn'] = union_no_npp_copy.groupby(['region_code_leads','branch_code_leads','subbranch_code_leads']).cumcount() + 1

    # Calculate max sales rank
    max_sales_rank = df_specific_sales_copy.groupby(['region_code_sales', 'branch_code_sales', 'subbranch_code_sales'])['rn'].max().reset_index()
    max_sales_rank.columns = ['region_code_sales', 'branch_code_sales', 'subbranch_code_sales', 'max_sales_rn']

    # Copy leads dataset to simplify
    leads_dataset_copy = union_no_npp_copy[['leads_id', 'region_code_leads', 'branch_code_leads', 'subbranch_code_leads', 'rn']]

    #### 1) ROUND ROBIN - REGION-BRANCH-subbranch
    # Merge leads with max sales rank
    prospects = pd.merge(leads_dataset_copy, max_sales_rank, 
                         right_on=['region_code_sales', 'branch_code_sales', 'subbranch_code_sales'], 
                         left_on=['region_code_leads', 'branch_code_leads', 'subbranch_code_leads'], 
                         how='left')
    prospects['adjusted_rn'] = (prospects['rn'] - 1) % prospects['max_sales_rn'] + 1

    # Split into assigned and not assigned prospects
    prospects_not_assigned = prospects[prospects['region_code_sales'].isnull()]
    prospects_assigned = prospects[~prospects['region_code_sales'].isnull()]

    # Handle not assigned prospects
    prospects_not_assigned = prospects_not_assigned[['leads_id', 'region_code_leads', 'branch_code_leads', 'subbranch_code_leads', 'rn']]
    prospects_not_assigned['subbranch_code_leads'] = 0.0

    #### 2) ROUND ROBIN - subbranch=0
    # Merge not assigned prospects with max sales rank
    prospects_not_assigned = pd.merge(prospects_not_assigned, max_sales_rank, 
                                      right_on=['region_code_sales', 'branch_code_sales', 'subbranch_code_sales'], 
                                      left_on=['region_code_leads', 'branch_code_leads', 'subbranch_code_leads'], 
                                      how='left')
    prospects_not_assigned['adjusted_rn'] = (prospects_not_assigned['rn'] - 1) % prospects_not_assigned['max_sales_rn'] + 1

    # Split into already assigned and not assigned prospects yet
    prospects_not_assigned_notyet = prospects_not_assigned[prospects_not_assigned['region_code_sales'].isnull()]
    prospects_not_assigned_finallyassigned = prospects_not_assigned[~prospects_not_assigned['region_code_sales'].isnull()]


    #### 3) ROUND ROBIN - REGION-BRANCH
    # remake the rank, but now by branch
    df_specific_sales_copy_again = sales_dataset.copy()
    union_no_npp_copy_again = prospects_not_assigned_notyet.copy()

    # Rank the sales per subbranch_code
    df_specific_sales_copy_again['rn'] = df_specific_sales_copy_again.groupby(['region_code_sales','branch_code_sales']).cumcount() + 1

    # Rank the prospects per subbranch_code
    union_no_npp_copy_again['rn'] = union_no_npp_copy_again.groupby(['region_code_leads','branch_code_leads']).cumcount() + 1

    # Calculate max sales rank
    max_sales_rank_again = df_specific_sales_copy_again.groupby(['region_code_sales', 'branch_code_sales'])['rn'].max().reset_index()
    max_sales_rank_again.columns = ['region_code_sales', 'branch_code_sales', 'max_sales_rn']

    # Copy leads dataset for testing purposes
    leads_dataset_copy_again = union_no_npp_copy_again[['leads_id', 'region_code_leads', 'branch_code_leads', 'rn']]

    # Merge leads with max sales rank
    prospects_again = pd.merge(leads_dataset_copy_again, max_sales_rank_again, 
                         right_on=['region_code_sales', 'branch_code_sales'], 
                         left_on=['region_code_leads', 'branch_code_leads'], 
                         how='left')
    prospects_again['adjusted_rn'] = (prospects_again['rn'] - 1) % prospects_again['max_sales_rn'] + 1

    # Split into assigned and not assigned prospects
    prospects_again_not_assigned = prospects_again[prospects_again['region_code_sales'].isnull()]
    prospects_again_assigned = prospects_again[~prospects_again['region_code_sales'].isnull()]

    #### 4) SUMMARY
    print(f"""
    The result of Round Robin:
     - Assigned directly = {len(prospects_assigned):,} CIFs
     - Assigned with modify the subbranch into 0 value = {len(prospects_not_assigned_finallyassigned):,} CIFs
     - Assigned with branch-branch level = {len(prospects_again_assigned):,} CIFs
     - Not Assigned yet = {len(prospects_again_not_assigned):,} CIFs

     - Total leads for checking purposes = {len(prospects_assigned) + len(prospects_not_assigned_finallyassigned) + len(prospects_again_assigned) + len(prospects_again_not_assigned):,} CIFs
    """)

    #### 5) REUNITE
    # Combine assigned and not assigned prospects
    prospect_revised_levelsubbranch = pd.concat([prospects_assigned, prospects_not_assigned_finallyassigned])
    prospect_revised_levelbranch = prospects_again_assigned

    # (LEVEL subbranch) Merge the ranked prospects with the ranked sales using adjusted_rn 
    assigned_prospects_levelsubbranch = pd.merge(prospect_revised_levelsubbranch, df_specific_sales_copy, 
                                  left_on=['region_code_sales', 'branch_code_sales', 'subbranch_code_sales', 'adjusted_rn'], 
                                  right_on=['region_code_sales', 'branch_code_sales', 'subbranch_code_sales', 'rn'], 
                                  how='left')

    # (LEVEL branch) Merge the ranked prospects with the ranked sales using adjusted_rn 
    assigned_prospects_levelbranch = pd.merge(prospect_revised_levelbranch, df_specific_sales_copy_again, 
                                  left_on=['region_code_sales', 'branch_code_sales', 'adjusted_rn'], 
                                  right_on=['region_code_sales', 'branch_code_sales', 'rn'], 
                                  how='left')

    # SIMPLIFY THE COLUMNS
    assigned_prospects_levelbranch['subbranch_code_sales'] = 0.0
    assigned_prospects_levelbranch_simplify = assigned_prospects_levelbranch[['leads_id','sales_id','sales_type','region_code_sales','branch_code_sales','subbranch_code_sales']]
    assigned_prospects_levelsubbranch_simplify = assigned_prospects_levelsubbranch[['leads_id','sales_id','sales_type','region_code_sales','branch_code_sales','subbranch_code_sales']]

    # UNION
    assigned_prospects_all = pd.concat([assigned_prospects_levelbranch_simplify, assigned_prospects_levelsubbranch_simplify])

    # MERGE INTO COMPLETION
    union_no_npp_merged = union_no_npp_copy.merge(assigned_prospects_all, on = 'leads_id', how = 'left')

    # SPLIT INTO 2 SO THE 'NOT ASSIGNED' BECOME "FREE" LEADS
    union_no_npp_merged_notassigned = union_no_npp_merged[union_no_npp_merged['sales_id'].isnull()]
    union_no_npp_merged_assigned = union_no_npp_merged[union_no_npp_merged['sales_id'].notna()]

    union_no_npp_merged_notassigned_adjcols = union_no_npp_merged_notassigned[['leads_id', 'customer_name', 'region_code_leads', 'branch_code_leads', 'subbranch_code_leads',
           'desc_1', 'desc_2', 'desc_3', 'desc_4', 'desc_5',
           'desc_6', 'desc_7', 'product_name',
           'desc_8', 'start_date', 'expired_date',
           'desc_9', 'desc_10', 'sales_id', 'flag_salestype_1', 'flag_salestype_2',
           'flag_salestype_3', 'flag_salestype_4', 'flag_salestype_5', 'flag_salestype_6', 'flag_salestype_7',
           'flag_salestype_8', 'flag_salestype_9', 'flag_salestype_10', 'flag_process']]

    union_no_npp_merged_assigned_adjcols = union_no_npp_merged_assigned[['leads_id', 'customer_name', 'region_code_sales', 'branch_code_sales', 'subbranch_code_sales',
           'desc_1', 'desc_2', 'desc_3', 'desc_4', 'desc_5',
           'desc_6', 'desc_7', 'product_name',
           'desc_8', 'start_date', 'expired_date',
           'desc_9', 'desc_10', 'sales_id', 'flag_salestype_1', 'flag_salestype_2',
           'flag_salestype_3', 'flag_salestype_4', 'flag_salestype_5', 'flag_salestype_6', 'flag_salestype_7',
           'flag_salestype_8', 'flag_salestype_9', 'flag_salestype_10', 'flag_process']]

    union_no_npp_merged_assigned_adjcols.rename(columns = {
        'sales_id' : 'sales_id',
        'region_code_sales' : 'region_code_leads',
        'branch_code_sales' : 'branch_code_leads',
        'subbranch_code_sales' : 'subbranch_code_leads'
    }, inplace = True)

    union_no_npp_merged_all = pd.concat([union_no_npp_merged_assigned_adjcols, union_no_npp_merged_notassigned_adjcols])

    return union_no_npp_merged_all 

# Creating a sample dataset for lead dataset
data = {
    'leads_id': ['e4818062-1c77-4d61-93fa-1b928e9f21a0', 'fd361a34-86e0-451d-b068-2bc881236ee1', 
                 'b50e8145-db2f-45de-b596-42808de57a7b', 'a103f9d3-9567-4773-9bc1-5e17caa08dde',
                 'b206340d-0421-4c64-beb9-3f6624cd9676'],
    'customer_name': ['Cathy Gates', 'James Hutchinson', 'Ryan Daniel', 'Kristina Hall', 'Jeffrey Jones'],
    'region_code_leads': ['R01', 'R03', 'R02', 'R02', 'R01'],
    'branch_code_leads': ['B01', 'B02', 'B02', 'B03', 'B01'],
    'subbranch_code_leads': ['SB03', 'SB03', 'SB03', 'SB02', 'SB02'],
    'desc_1': ['Structure letter laugh food indeed.', 'Wrong particularly difference.', 
               'West notice project bit close.', 'House conference play.', 'Become power player poor statement.'],
    'desc_2': ['Interview chair majority.', 'Difficult in local.', 'System although civil short.', 
               'Quickly Republican member.', 'Story discuss house.'],
    'desc_3': ['...', '...', '...', '...', '...'],
    'desc_4': ['...', '...', '...', '...', '...'],
    'desc_5': ['...', '...', '...', '...', '...'],
    'desc_6': ['...', '...', '...', '...', '...'],
    'desc_7': ['...', '...', '...', '...', '...'],
    'product_name': ['Product A', 'Product B', 'Product C', 'Product A', 'Product D'],
    'desc_8': ['...', '...', '...', '...', '...'],
    'start_date': ['2023-01-01', '2023-03-15', '2023-06-20', '2023-02-10', '2023-04-30'],
    'expired_date': ['2023-12-31', '2023-10-15', '2023-11-20', '2023-09-10', '2023-08-30'],
    'desc_9': ['...', '...', '...', '...', '...'],
    'desc_10': ['...', '...', '...', '...', '...'],
    'sales_id': ['S001', 'S002', 'S003', 'S001', 'S004'],
    'flag_salestype_1': [1, 0, 1, 1, 0],
    'flag_salestype_2': [0, 1, 0, 0, 1],
    'flag_salestype_3': [1, 0, 0, 1, 0],
    'flag_salestype_4': [0, 0, 0, 1, 1],
    'flag_salestype_5': [1, 1, 0, 0, 1],
    'flag_salestype_6': [0, 0, 0, 0, 0],
    'flag_salestype_7': [1, 1, 0, 1, 0],
    'flag_salestype_8': [0, 0, 1, 0, 0],
    'flag_salestype_9': [0, 0, 0, 1, 0],
    'flag_salestype_10': [1, 1, 0, 0, 0],
    'flag_process': [0, 1, 1, 1, 0]
}

leads_df = pd.DataFrame(data)

# Creating a sample dataset for sales dataset
sales_data = {
    'sales_id': ['S001', 'S002', 'S003', 'S004', 'S005'],
    'sales_type': ['Retail', 'Wholesale', 'Online', 'Retail', 'Online'],
    'region_code_sales': ['R01', 'R03', 'R02', 'R01', 'R03'],
    'branch_code_sales': ['B01', 'B02', 'B03', 'B01', 'B02'],
    'subbranch_code_sales': ['SB01', 'SB02', 'SB03', 'SB01', 'SB02']
}

sales_df = pd.DataFrame(sales_data)

# Enhance Leads with sales id based on region-branch-subbranch
sales_assigned_df = round_robin_leads_sales(leads_df, sales_df)
sales_assigned_df
