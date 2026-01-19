# Twitch Stream Data Pipeline on AWS

## Overview
This project is an end-to-end data pipeline hosted on AWS that ingests, processes, and stores data from the Twitch and IGDB APIs. The goal is to store and view historical stream data through multiple contexts such as time, broadcaster, category, genre, and game mode. The pipeline runs and collects data every 15 minutes and adopts an architecture similar to the medallion architecture where data is stored in a raw, processed, and curated layer. As of January 19, over 40,000,000 stream data points have been collected. AWS services such as Lambda, Simple Notification Service, Simple Queue Service, PostgreSQL, S3, Quicksight, and VPC are used in this project.

### Quicksight Dashboard Data Visualization
![Dashboard Visualization Example 1](diagrams/dashboard_visualization1.png)

![Dashboard Visualization Example 2](diagrams/dashboard_visualization2.png)

### Data Pipeline Architecture


### Database Data Model
![Data model for Postgres RDS](diagrams/twitch_stream_data_model.png)

