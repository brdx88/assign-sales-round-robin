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
