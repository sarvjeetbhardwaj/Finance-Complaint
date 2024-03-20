 # Finance-Complaint 

### Problem Statement
Complaints can give us insights into problems people are experiencing in the marketplace and help us to undestand the reason and do necessary modification in exisiting financial product if required.

### Solution Proposed 
By understanding existing complaints registered against financial products we can create an ML model that can help us to identify newly registered complaints whether they are problematic or not and accordingly company can take quick action to resolve the issue, and satisfy the customer's need.

The problem is to identify registered complaint will be disputed by customer or not.

## Data Source
https://www.consumerfinance.gov/data-research/consumer-complaints/

## sample link to download data
https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=2022-04-29&date_received_min=2022-01-01&field=all&format=json&no_aggs=true&size=238777&sort=created_date_desc

## TechStack used
 - Python
 - PySpark
 - Docker
 - Airflow
 - AWS S3
 - AWS EMR
 - AWS EC2

## To build docker image
    docker build --network=host .

## To run docker image
    docker run -p 8080:8080 -v $(pwd)/airflow/dags:/app/airflow/dags finance:latest

## Project Workflow
![alt text](<Training Pipeline.png>)