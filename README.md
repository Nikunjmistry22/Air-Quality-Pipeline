# Air Quality Pipeline

This project aims to develop a comprehensive data pipeline for monitoring and visualizing air quality data. The system extracts air quality data from the Air Visuals API, loads it into a PostgreSQL database, transforms the data using DBT, and visualizes it using BI tools such as Metabase, Power BI, or Tableau. The goal is to provide real-time and historical insights into air quality, enabling better decision-making and awareness.

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

<h4>Run the `states.py` file</h4>
This will fetch all the city data where air quality data centers are present.

<h4>Run the `main.py` file to execute your pipeline.</h4>

<h4>To orchestrate, you can use any orchestration tools for daily change captures.</h4>
