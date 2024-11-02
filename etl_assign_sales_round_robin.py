from pyspark.sql import SparkSession, functions as F, Window

# Initialize Spark session
spark = SparkSession.builder.appName("ETLRoundRobinLeadsSales").getOrCreate()

# Sample leads dataset
leads_data = [
    ('e4818062-1c77-4d61-93fa-1b928e9f21a0', 'Cathy Gates', 'R01', 'B01', 'SB03', 'Product A', '2023-01-01', '2023-12-31', 'S001', 1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0),
    ('fd361a34-86e0-451d-b068-2bc881236ee1', 'James Hutchinson', 'R03', 'B02', 'SB03', 'Product B', '2023-03-15', '2023-10-15', 'S002', 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 1),
    ('b50e8145-db2f-45de-b596-42808de57a7b', 'Ryan Daniel', 'R02', 'B02', 'SB03', 'Product C', '2023-06-20', '2023-11-20', 'S003', 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0),
    ('a103f9d3-9567-4773-9bc1-5e17caa08dde', 'Kristina Hall', 'R02', 'B03', 'SB02', 'Product A', '2023-02-10', '2023-09-10', 'S001', 1, 0, 1, 1, 0, 0, 1, 0, 0, 1, 0),
    ('b206340d-0421-4c64-beb9-3f6624cd9676', 'Jeffrey Jones', 'R01', 'B01', 'SB02', 'Product D', '2023-04-30', '2023-08-30', 'S004', 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0)
]
leads_schema = ['leads_id', 'customer_name', 'region_code_leads', 'branch_code_leads', 'subbranch_code_leads', 
                'product_name', 'start_date', 'expired_date', 'sales_id', 
                'flag_salestype_1', 'flag_salestype_2', 'flag_salestype_3', 'flag_salestype_4', 'flag_salestype_5', 
                'flag_salestype_6', 'flag_salestype_7', 'flag_salestype_8', 'flag_salestype_9', 'flag_salestype_10', 'flag_process']
leads_df = spark.createDataFrame(leads_data, schema=leads_schema)

# Sample sales dataset
sales_data = [
    ('S001', 'Retail', 'R01', 'B01', 'SB01'),
    ('S002', 'Wholesale', 'R03', 'B02', 'SB02'),
    ('S003', 'Online', 'R02', 'B03', 'SB03'),
    ('S004', 'Retail', 'R01', 'B01', 'SB01'),
    ('S005', 'Online', 'R03', 'B02', 'SB02')
]
sales_schema = ['sales_id', 'sales_type', 'region_code_sales', 'branch_code_sales', 'subbranch_code_sales']
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Function to perform round-robin assignment of leads to sales based on region-branch-subbranch
def round_robin_leads_sales(leads_df, sales_df):
    # Add row number to sales per subbranch
    window_sales = Window.partitionBy("region_code_sales", "branch_code_sales", "subbranch_code_sales").orderBy("sales_id")
    sales_df = sales_df.withColumn("rn", F.row_number().over(window_sales))
    
    # Add row number to leads per subbranch
    window_leads = Window.partitionBy("region_code_leads", "branch_code_leads", "subbranch_code_leads").orderBy("leads_id")
    leads_df = leads_df.withColumn("rn", F.row_number().over(window_leads))
    
    # Calculate max sales rank for each subbranch
    max_sales_rank = sales_df.groupBy("region_code_sales", "branch_code_sales", "subbranch_code_sales") \
                             .agg(F.max("rn").alias("max_sales_rn"))
    
    # Merge leads with max sales rank
    prospects = leads_df.join(max_sales_rank,
                              (leads_df.region_code_leads == max_sales_rank.region_code_sales) & 
                              (leads_df.branch_code_leads == max_sales_rank.branch_code_sales) & 
                              (leads_df.subbranch_code_leads == max_sales_rank.subbranch_code_sales),
                              "left") \
                        .withColumn("adjusted_rn", (F.col("rn") - 1) % F.col("max_sales_rn") + 1)
    
    # Handle not assigned prospects
    prospects_not_assigned = prospects.filter(F.col("region_code_sales").isNull()) \
                                       .withColumn("subbranch_code_leads", F.lit(0.0))
    
    # Second round robin for unassigned prospects (subbranch = 0)
    prospects_not_assigned = prospects_not_assigned.join(max_sales_rank,
                                                         (prospects_not_assigned.region_code_leads == max_sales_rank.region_code_sales) & 
                                                         (prospects_not_assigned.branch_code_leads == max_sales_rank.branch_code_sales) & 
                                                         (prospects_not_assigned.subbranch_code_leads == max_sales_rank.subbranch_code_sales),
                                                         "left") \
                                                   .withColumn("adjusted_rn", (F.col("rn") - 1) % F.col("max_sales_rn") + 1)
    
    # Calculate branch-level rank for remaining unassigned prospects
    window_sales_branch = Window.partitionBy("region_code_sales", "branch_code_sales").orderBy("sales_id")
    sales_df_branch = sales_df.withColumn("rn", F.row_number().over(window_sales_branch))
    
    max_sales_rank_branch = sales_df_branch.groupBy("region_code_sales", "branch_code_sales") \
                                           .agg(F.max("rn").alias("max_sales_rn"))
    
    prospects_branch = prospects_not_assigned.join(max_sales_rank_branch,
                                                   (prospects_not_assigned.region_code_leads == max_sales_rank_branch.region_code_sales) & 
                                                   (prospects_not_assigned.branch_code_leads == max_sales_rank_branch.branch_code_sales),
                                                   "left") \
                                             .withColumn("adjusted_rn", (F.col("rn") - 1) % F.col("max_sales_rn") + 1)
    
    # Combine assigned prospects
    assigned_prospects = prospects.filter(F.col("region_code_sales").isNotNull()) \
                                   .unionByName(prospects_branch.filter(F.col("region_code_sales").isNotNull()))

    # Join with sales data for final assignment
    final_assigned_prospects = assigned_prospects.join(sales_df, 
                                                       (assigned_prospects.region_code_sales == sales_df.region_code_sales) & 
                                                       (assigned_prospects.branch_code_sales == sales_df.branch_code_sales) & 
                                                       (assigned_prospects.subbranch_code_sales == sales_df.subbranch_code_sales) & 
                                                       (assigned_prospects.adjusted_rn == sales_df.rn), 
                                                       "left") \
                                                 .select("leads_id", "sales_id", "sales_type", "region_code_sales", 
                                                         "branch_code_sales", "subbranch_code_sales")
    
    return final_assigned_prospects

# Execute function and show results
sales_assigned_df = round_robin_leads_sales(leads_df, sales_df)
sales_assigned_df.show()
