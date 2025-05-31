# Stack Exchange User Engagement Analysis

**Authors:** Cindy (Di) Chen & Joshua Magana  
**Date:** March 11, 2023  

## 📌 Overview

Stack Exchange provides public access to its platform data via API. This project performs an in-depth analysis of user engagement trends across the platform, focusing on:

- Comment activity by post type
- Average posts and comments per user over time
- Answer trends and accepted answers
- User creation patterns by year

Our goal is to identify factors that impact engagement, and provide actionable insights to help Stack Exchange improve user retention and platform activity.

---

## 🧠 Problem Statement

By analyzing Stack Exchange activity data, we aim to understand how different types of user interactions contribute to platform engagement. Specifically, we ask:

> What patterns in user behavior (posts, comments, answers) are linked to higher engagement?

---

## 🔍 Data Acquisition

### ✅ Source
- **Stack Exchange Public Data Dump** via [API](https://archive.org/details/stackexchange)
- Data format: XML

### ✅ Access & Storage
- **Tools:** Python (API download), Google Cloud Storage, Google BigQuery
- Joshua set up the GCP project and shared access with Cindy
- Raw files stored in GCP; transformed datasets stored in BigQuery under:


---

## 🧹 Data Wrangling

### 1. Pipeline Tasks

| Step | Description |
|------|-------------|
| 1 | Pull XML data using Python |
| 2 | Store raw files in Google Cloud Storage |
| 3 | Use **Scala + Spark** to clean, process, and repartition the data |
| 4 | Load into **BigQuery** with defined schemas |
| 5 | Create an ERD model |
| 6 | Connect BigQuery to Power BI |
| 7 | Define business questions |
| 8 | Build dashboards to visualize findings |

### 2. Tools & Scripts

- **Scala Spark Script:** Created DataFrames, temporary views, repartitioned data
- **Python Script:** Used for SSH download and data staging
- **BigQuery Tables:**
- `badges`
- `comments`
- `post_history`
- `post_links`
- `posts`
- `tags`
- `users`
- `votes`

### 3. Notable Challenges

- PySpark was originally used, but failed due to XML parsing and JAR conflicts
- Resolved using Scala Spark with correct dependencies
- Access permissions in GCP needed troubleshooting to allow Cindy full access

---

## 📊 Data Exploration & Visualizations

### I. 💬 Number of Comments by Post Type

**Insights:**
- **Answers** have the highest number and percentage of comments, followed by **Questions**
- Post types like *Moderator Nomination* and *Tag Wiki* have much lower engagement
- Comments on answers and questions remain consistent across years

**Visualization Types:** Donut chart, bar chart, line chart, crosstab

**Conclusion:**  
Answers and questions drive the most engagement. However, some years have missing data, so trends should be interpreted cautiously.

---

### II. 📈 Average Posts and Comments Per User by Year

**Insights:**
- Avg. **comments per user** decreased until 2015, then rose again through 2022
- Avg. **posts per user** shows a gradual decline from 2010–2022

**Conclusion:**  
Although users are commenting more recently, overall posting has declined, suggesting engagement shifts.

---

### III. ✅ Answer Count & Accepted Answers per Year

**Insights:**
- Both metrics declined consistently since 2010
- Lower answer volumes directly impact accepted answers

**Conclusion:**  
A downward trend in answers indicates waning user engagement or content saturation.

---

### IV. 👤 Number of Users Created per Year

**Insights:**
- User creation peaked in 2015–2016
- Significant decline from 2017–2022
- Answer & question post types attract the most users

**Conclusion:**  
Attracting users via engaging Q&A content could help reverse this trend.

---

## 🧪 Modeling Approach

We used **descriptive analytics** to explore patterns in user activity. No predictive modeling was performed. An **ERD** was created to visualize table relationships and support schema design.

![ERD Diagram](https://i.stack.imgur.com/AyIkW.png)

---

## 💡 Interpretation & Recommendations

### What we learned:
- **Engaging post types:** Answers and Questions
- **Declining activity:** Fewer answers and accepted answers over time
- **User growth slowing:** New user registrations declining post-2016
- **Data quality:** Some missing data between years, especially for niche post types

### Recommendations for Stack Exchange:
1. Encourage more Q&A content to boost engagement
2. Solicit feedback to tailor content to user interests
3. Investigate reasons for user churn post-2016

---

## 🧩 Collaboration & Acknowledgements

- **Cindy Chen:** Data wrangling, Power BI visualization, business question framing  
- **Joshua Magana:** GCP setup, API download, Scala Spark ETL pipeline

Special thanks to the OSU CS-512 course for guidance.

---

## 🔗 References

- [Stack Exchange Data Dump Schema](https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede)
- [ERD Diagram Reference](https://i.stack.imgur.com/AyIkW.png)

---

## ✅ Summary

This project demonstrates the end-to-end analytics pipeline from data extraction (API + XML) to cloud ETL (Scala Spark + BigQuery) and dashboard visualization (Power BI). Through our analysis, we identified actionable trends that could help Stack Exchange improve user engagement and platform growth.

