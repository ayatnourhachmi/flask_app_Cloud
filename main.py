from flask import Flask, request, render_template, redirect, url_for
from google.cloud import storage, bigquery

app = Flask(__name__)

# Configure the GCS bucket name
GCS_BUCKET_NAME = 'etl-input-output-bucket'

# Initialize the Google Cloud Storage and BigQuery clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Helper function to run BigQuery queries
def run_bigquery_query(query):
    query_job = bigquery_client.query(query)
    return [dict(row) for row in query_job] 

# Function to clean and re-rank data in BigQuery
def clean_and_rerank_data():
    query = """
    -- Clean and Re-Rank Data
    WITH FilteredData AS (
      SELECT *
      FROM `vgsales-445219.vgsalesProject.vgsales`
      WHERE SAFE_CAST(Year AS INT64) IS NOT NULL
        AND Publisher IS NOT NULL
        AND LOWER(Publisher) != 'n/a'
    ),
    ReRankedData AS (
      SELECT
        ROW_NUMBER() OVER (ORDER BY Global_Sales DESC, Name ASC) AS Rank,
        Name,
        Platform,
        CAST(Year AS INT64) AS Year,
        Genre,
        Publisher,
        NA_Sales,
        EU_Sales,
        JP_Sales,
        Other_Sales,
        Global_Sales
      FROM FilteredData
    )
    SELECT *
    FROM ReRankedData;
    """
    
    # Destination table for cleaned data
    destination_table = "vgsales-445219.vgsalesProject.vgsalesCleaned"
    
    # Configure the query to write the result to a new table
    job_config = bigquery.QueryJobConfig(destination=destination_table)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite the table if it exists

    # Run the query
    query_job = bigquery_client.query(query, job_config=job_config)
    query_job.result()  # Wait for the job to complete

    print(f"Data cleaned and re-ranked. Saved to {destination_table}.")

# File upload route
@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return 'No file part'
        file = request.files['file']
        if file.filename == '':
            return 'No selected file'
        if file:
            # Upload the file to the 'raw/' folder in GCS
            bucket = storage_client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(f'raw/{file.filename}')  # Add 'raw/' prefix
            blob.upload_from_file(file)

            # Trigger data cleaning and re-ranking after upload
            clean_and_rerank_data()

            return f'File {file.filename} uploaded and data cleaned/re-ranked successfully.'
    return render_template('upload.html')

# Main app route with filtering options
@app.route('/', methods=['GET', 'POST'])
def index():
    filter_type = request.form.get("filter_type", None)
    selected_value = request.form.get("selected_value", "").strip()  # Strip whitespace

    # Define base query
    query = None
    if filter_type == "Genre":
        if selected_value:  # Show all games in the specific Genre
            query = f"""
            SELECT DISTINCT
              Genre,
              Name AS Best_Game,
              Year,
              Global_Sales AS Best_Sales
            FROM 
              `vgsales-445219.vgsalesProject.vgsalesCleaned`
            WHERE Genre = '{selected_value}'
            ORDER BY Best_Sales DESC;
            """
        else:  # Show the best game for each Genre
            query = """
            SELECT DISTINCT
              t1.Genre,
              t1.Name AS Best_Game,
              t1.Year,
              t1.Global_Sales AS Best_Sales
            FROM 
              `vgsales-445219.vgsalesProject.vgsalesCleaned` t1
            WHERE 
              t1.Global_Sales = (
                SELECT MAX(t2.Global_Sales)
                FROM `vgsales-445219.vgsalesProject.vgsalesCleaned` t2
                WHERE t2.Genre = t1.Genre
              )
            ORDER BY Best_Sales DESC;
            """
    elif filter_type == "Platform":
        if selected_value:
            query = f"""
            SELECT DISTINCT
              Platform,
              Name AS Best_Game,
              Year,
              Global_Sales AS Best_Sales
            FROM 
              `vgsales-445219.vgsalesProject.vgsalesCleaned`
            WHERE Platform = '{selected_value}'
            ORDER BY Best_Sales DESC;
            """
        else:
            query = """
            SELECT DISTINCT
              t1.Platform,
              t1.Name AS Best_Game,
              t1.Year,
              t1.Global_Sales AS Best_Sales
            FROM 
              `vgsales-445219.vgsalesProject.vgsalesCleaned` t1
            WHERE 
              t1.Global_Sales = (
                SELECT MAX(t2.Global_Sales)
                FROM `vgsales-445219.vgsalesProject.vgsalesCleaned` t2
                WHERE t2.Platform = t1.Platform
              )
            ORDER BY Best_Sales DESC;
            """
    elif filter_type == "Publisher":
        if selected_value:
            query = f"""
            SELECT DISTINCT
              Publisher,
              Name AS Best_Game,
              Year,
              Global_Sales AS Best_Sales
            FROM 
              `vgsales-445219.vgsalesProject.vgsalesCleaned`
            WHERE Publisher = '{selected_value}'
            ORDER BY Best_Sales DESC;
            """
        else:
            query = """
            SELECT DISTINCT
              t1.Publisher,
              t1.Name AS Best_Game,
              t1.Year,
              t1.Global_Sales AS Best_Sales
            FROM 
              `vgsales-445219.vgsalesProject.vgsalesCleaned` t1
            WHERE 
              t1.Global_Sales = (
                SELECT MAX(t2.Global_Sales)
                FROM `vgsales-445219.vgsalesProject.vgsalesCleaned` t2
                WHERE t2.Publisher = t1.Publisher
              )
            ORDER BY Best_Sales DESC;
            """

    # Execute query if defined
    results = run_bigquery_query(query) if query else []
    print(results)

    # Render the template with results
    return render_template("index.html", results=results, filter_type=filter_type, selected_value=selected_value)


if __name__ == '__main__':
  app.run(host="0.0.0.0", port=8080)

