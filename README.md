# Air Quality Pipeline

This project is designed to extract, transform, and load (ETL) air quality data into a Supabase PostgreSQL database. The data is processed to provide insights into air quality across various cities. This pipeline is built using Python for extraction, data transformation, and automation tools, and Supabase PostgreSQL is used as the backend database.

# Features
<ul>
    <li>Data Extraction: Fetches air quality data from external APIs.</li>
    <li>Data Transformation: Cleans and transforms the data to match database schema requirements.</li>
    <li>Database Integration: Loads processed data into Supabase PostgreSQL.</li>
    <li>Automation: The pipeline can be scheduled to run periodically to ensure up-to-date data ingestion.</li>
</ul>

# Tech Stack
<ul>
    <li>Language: Python</li>
    <li>Database: Supabase PostgreSQL</li>
    <li>APIs: Air Visuals</li>
    <li>Automation: Airflow (optional), Cron jobs, or custom scheduling scripts.</li>
</ul>

# Setup Guide
Prerequisites
Ensure you have the following installed:
<ul>
<li>Python 3.x
<li>Supabase Account with PostgreSQL Database</li>
<li>API key for air quality data AirVisuals</li>
</ul>

# High-Level Architecture
![image](https://github.com/user-attachments/assets/dfe790e7-bed3-491c-a136-233961505321)

# Low-Level Design
<ol>
<li>Sign in to the IQAir website and go to the dashboard to create an API key for the Community plan.</li>
<li>Write the extraction code using the AirVisuals API documentation.</li>
<li>First, get all the states and then cities data in JSON format to determine which states and cities have available data.</li>
<li>Remove cities from states where the city response fails.</li>
<li>After running this code, you will have all state and city names in JSON format.</li>
<li>Create a `constants.py` file to get the environment variables, i.e., API_KEY and Database URI.</li>
</ol>

<h4>First, create a .env file that contains two main parameters:</h4>
<ul>
    <li>API_KEY</li>
    <li>DATABASE_URI for PostgreSQL</li>
</ul>

<h4>Run the `states.py` file and store the result as "test_data.json"</h4>
This will fetch all the city data where air quality data centers are present.

<h4>Run the `main.py` file to execute your pipeline.</h4>

<h4>To orchestrate, you can use any orchestration tools for daily change captures.</h4>

# Dimensions & Facts Tables
![image](https://github.com/user-attachments/assets/9ca7fdfd-941e-4d3e-924f-6c793ab40132)
