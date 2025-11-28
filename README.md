# INF2006-Project- 
Step 1: Cluster Connection
Securely connect to the EMR master node using SSH with local port forwarding to access the Zeppelin interface.

Command: ssh -i "~/labsuser.pem" -L 8890:localhost:8890 -L 5000:localhost:5000 hadoop@<emr-dns> 

Port 8890: Forwarded for Apache Zeppelin.
Port 5000: Forwarded for application UIs.

Verification: Navigate to localhost:8890 in a web browser to see the Zeppelin Welcome screen.

Step 2: Data Ingestion
Transfer the dataset from the local machine to the EMR cluster and then into the distributed file system.

SCP Transfer: Upload the file from the local machine to the EMR master node's local Linux filesystem.
scp -i "labsuser.pem" consolidated_market_datasets_final.csv hadoop@<emr-dns>:~/ 

Verify Upload: Ensure the file exists on the Linux node.

ls -l consolidated_market_datasets_final1.csv 

HDFS Ingestion: Create a directory in HDFS and move the file from the Linux filesystem to HDFS.

hadoop fs -mkdir -p /user/hadoop/sales_data/ 
hadoop fs -put consolidated_market_datasets_final1.csv /user/hadoop/sales_data/ 

Step 3: Exploratory Data Analysis (EDA)
Perform data analysis using a Zeppelin Notebook (SalesAnalysisPipeline).

Schema Definition: Define a strict schema using StructType to ensure data types (String, Double, Integer) are correctly interpreted.

Data Loading: Load the CSV from HDFS (hdfs:///user/hadoop/sales_data/...) into a Spark DataFrame.

Visualization (SQL):

Execute Spark SQL queries to calculate average price and average rating by category.

Aggregation (DataFrame API):

Group data by platform to calculate total_units_sold and total_transactions.

Step 4: Storage & Verification
Save the processed insights back to storage for future use.

Write to Parquet: Save the aggregated DataFrame to HDFS in Parquet format with overwrite mode.

Path: /user/hadoop/results/platform_sales_insights.

Verification: Verify the output files were successfully created in HDFS via the terminal.

hadoop fs -ls /user/hadoop/results/platform_sales_insights/ 

Result confirms the existence of the _SUCCESS flag and the .parquet data file.
