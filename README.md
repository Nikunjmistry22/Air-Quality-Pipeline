# Air-Quality-Pipeline
This project aims to develop a comprehensive data pipeline for monitoring and visualizing air quality data. The system extracts air quality data from the Air Visuals API, loads it into a PostgreSQL database, transforms the data using DBT, and visualizes it using BI tools such as METABASE, POWER BI, or TABLEAU. The goal is to provide real-time and historical insights into air quality, enabling better decision-making and awareness.

# High Level Architecture
![image](https://github.com/user-attachments/assets/dfe790e7-bed3-491c-a136-233961505321)

# Low Level Design
<ol>
<li>Sign in to the IQAir website and go to the dashboard to create an API key for the Community plan.</li>
<li>Write the extraction code using the AirVisuals API documentation.</li>
<li>First, get all the states and than cities data in JSON format to determine which states and cities have available data.</li>
<li>Remove cities from states where the city response fails.</li>
<li>After running this code, you will get all state and city names in JSON format.</li>
<li>Create a constant.py to get  the env variables ie api_key and Database URI</li>
</ol>

<h2>Run the file states.py</h2>
