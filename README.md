Overview
dbETLForBestTimeToRun is the ETL (Extract, Transform, Load) pipeline powering the BestTimeToRun App. It automates the process of collecting, cleaning, and loading running-related data, enabling the app to provide insights on optimal running times.

Features

Automated ETL Pipeline: Extracts, transforms, and loads data with minimal manual intervention.
Scheduled Execution: Runs automatically on Vercel using serverless cron jobs.
Supabase Integration: Securely stores processed data in Supabase, making it accessible to the app and other services.
Flexible Data Sources: Supports multiple input types (APIs, CSVs, databases, etc.).
Data Cleaning & Enrichment: Transforms raw data into reliable, actionable information.

Deployment & Automation
The ETL process is scheduled and triggered via a cron job on Vercel, ensuring regular and reliable data updates.
After processing, all data is loaded directly into a Supabase database for storage and retrieval by the BestTimeToRun app.
