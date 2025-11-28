from flask import Flask, request, render_template, jsonify
import requests
import json
import time

app = Flask(__name__)
# Livy runs on port 8998 by default on EMR
LIVY_URL = "http://localhost:8998" 

# --- CONSTANTS ---
# Schema matching consolidated_market_datasets_final.csv
SCHEMA_STRING = 'product_name STRING, brand STRING, price DOUBLE, rating STRING, category STRING, number_sold INT, platform STRING, product_link STRING'
# CRITICAL: This points to the new file you uploaded
DATA_PATH = '/user/hadoop/sales_data/consolidated_market_datasets_final1.csv' 
# -----------------

@app.route('/')
def index():
    """Renders the HTML frontend."""
    return render_template('index.html')

@app.route('/run_job', methods=['POST'])
def run_job():
    """Handles the AJAX request, submits PySpark job via Livy, and returns JSON results."""
    
    headers = {'Content-Type': 'application/json'}
    session_id = None
    
    # 1. === CREATE A NEW, FRESH SPARK SESSION ===
    try:
        session_data = {'kind': 'pyspark'}
        r_session = requests.post(LIVY_URL + '/sessions', data=json.dumps(session_data), headers=headers)
        r_session.raise_for_status()
        session_id = r_session.json()['id']
        session_url = f"{LIVY_URL}/sessions/{session_id}"
    except Exception as e:
        # If Livy cannot be reached, the session won't be created.
        return jsonify(error=f"Could not create Livy session (Check EMR status & SSH tunnel): {e}"), 500
    
    # Wait for the new session to become idle
    start_time = time.time()
    while time.time() - start_time < 60: # Max wait 60s for session to start
        r_status = requests.get(session_url, headers=headers)
        if r_status.json().get('state') == 'idle':
            break
        time.sleep(1)
    
    # Check if the session is ready before proceeding
    if r_status.json().get('state') != 'idle':
        requests.delete(session_url, headers=headers)
        return jsonify(error="Spark session timed out during initialization."), 500
    
    # 2. === BUILD THE SPARK SCRIPT BASED ON USER'S QUERY ===
    query_name = request.form['query']
    spark_query = ""
    
    if query_name == 'platform_exposure':
        # Insight: Total Units Sold and Transaction Count by Platform (Exposure Gain)
        spark_query = f"""
            SELECT 
                platform,
                SUM(COALESCE(number_sold, 0)) AS total_units_sold,
                COUNT(product_name) AS transaction_count 
            FROM 
                sales_data_view
            GROUP BY 
                platform
            ORDER BY 
                total_units_sold DESC
        """
    elif query_name == 'category_performance':
        # Insight: Average Rating and Price by Category (Product Strategy)
        spark_query = f"""
            SELECT
                category,
                ROUND(AVG(COALESCE(price, 0)), 2) AS average_price,
                -- Safe conversion of rating using RLIKE to filter out non-numeric strings
                ROUND(
                    AVG(
                        CASE 
                            WHEN rating RLIKE '^[0-9]+\\.?[0-9]*$' THEN CAST(rating AS DOUBLE) 
                            ELSE NULL 
                        END
                    ), 
                    2
                ) AS average_rating
            FROM
                sales_data_view
            WHERE 
                category IS NOT NULL AND category != ''
            GROUP BY
                category
            HAVING 
                COUNT(product_name) > 10
            ORDER BY
                average_rating DESC
        """

    # PySpark script template to run on the EMR cluster
    pyspark_code_template = f"""
import json
from pyspark.sql.functions import col, sum as spark_sum, avg as spark_avg

# 1. Load the data using the predefined schema with robust CSV options
df = spark.read.format('csv') \\
    .schema('{SCHEMA_STRING}') \\
    .option('header', 'true') \\
    .option('quote', '\"') \\
    .option('escape', '\"') \\
    .load('{DATA_PATH}')
df.createOrReplaceTempView('sales_data_view')

# 2. Execute the user-selected SQL query
results_rows = spark.sql(\"\"\"{spark_query}\"\"\").collect()

# 3. Convert results to a list of dictionaries for JSON serialization
results_list = [row.asDict() for row in results_rows]
print(json.dumps(results_list))
    """
    
    # 3. === SUBMIT THE SCRIPT AND POLL FOR THE RESULT ===
    data = {'code': pyspark_code_template, 'kind': 'pyspark'}
    r_statement = requests.post(session_url + '/statements', data=json.dumps(data), headers=headers)
    
    # Handle failure to submit statement
    if r_statement.status_code != 201:
        requests.delete(session_url, headers=headers)
        return jsonify(error=f"Failed to submit statement to Livy: {r_statement.text}"), 500
        
    statement_url = f"{LIVY_URL}{r_statement.headers['location']}"
    
    start_time = time.time()
    while time.time() - start_time < 180: # 3 minute timeout for query execution
        r_result = requests.get(statement_url, headers=headers)
        response_json = r_result.json()
        
        if response_json['state'] == 'available':
            # --- Robust Cleanup Sequence (Ensures resources are freed) ---
            requests.delete(session_url, headers=headers) 
            time.sleep(2) 
            # -----------------------------------------------------------
            
            output = response_json.get('output', {})
            if output and output.get('status') == 'ok':
                json_string = output['data']['text/plain']
                return jsonify(result=json.loads(json_string))
            else:
                return jsonify(error=f"Spark job failed during execution: {output.get('evalue', 'Unknown error')}"), 500

        time.sleep(3) # Wait 3 seconds between status checks

    # Handle timeout
    requests.delete(session_url, headers=headers) 
    return jsonify(error="Job timed out after 3 minutes."), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)