📊 Stack Exchange User Engagement Analysis
Contributors: Cindy (Di) Chen & Joshua Magana
Date: March 11, 2023

🧠 Problem Statement
Stack Exchange provides a public API and data dumps in XML format. This project explores user engagement by analyzing trends in post types, comments, answers, and user creation over time. The goal is to identify the factors that drive user participation and inform strategies to increase engagement.

📦 Data Pipeline
Data Source: Stack Exchange API & public XML data dump
Storage & Processing:

Extracted XML data using Python and uploaded to Google Cloud Storage

Used Scala with Spark to parse, clean, and transform data

Loaded processed tables into BigQuery for analysis

Connected BigQuery to Power BI for visualization

Technologies Used:

Python, Scala, Apache Spark, Google Cloud (Storage & BigQuery), Power BI

🔧 Project Workflow
Pulled raw XML data via API

Parsed and processed XML into structured DataFrames (e.g., Posts, Users, Comments)

Repartitioned data and loaded into BigQuery

Built ERD to define relationships between entities

Created Power BI dashboards to answer business questions

📈 Key Findings
Most engaging post types: Answers and Questions dominate user activity

Comments per user: Increased steadily post-2015, indicating rising engagement

Answer & accepted answer counts: Declining trend from 2010–2022

User growth: Peaked in 2015–2016, then declined sharply

Insight: Targeting answer/question-type content and improving engagement strategies can help attract and retain users

📉 Challenges
Faced technical issues with PySpark XML processing

Resolved errors by switching to Scala with correct JAR dependencies

Addressed permission issues when accessing shared BigQuery schemas

🛠 Skills Applied
API integration & data extraction

XML parsing and transformation with Spark

BigQuery schema design & loading

ERD modeling

Power BI dashboard development

Descriptive statistics and trend analysis

📊 Visualizations
Comments by Post Type

Avg Posts & Comments per User by Year

Answer & Accepted Answers Over Time

User Growth Trends (2010–2022)
