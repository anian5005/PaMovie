# Side Project - PaMovie

PaMovie, is a data integration website related to films. Users can get movie info, including introductions, cast, plot summaries, and integrated ratings from IMDb, Douban, and RottenTomatoes on a single website.

Website : https://pamovie.com/  

Guest Password For Dashboard: 2022

![home_page](https://user-images.githubusercontent.com/97599669/194929062-c2d0e895-cf29-4f59-bb6c-b1031dfe77a9.png)


## Data Pipeline
### 1. Movie ID & static information pipeline
Using Airflow to automate ETL pipeline, which matched movie ID and fetch static hotel information.

![dags_update_data](https://user-images.githubusercontent.com/97599669/197456223-9af69d46-2604-430c-acfd-04f4a5c671d8.png)

### 2. Movie ratings pipeline
Built an ETL pipeline to scrape rating data from various sources including IMDb, Rotten Tomatoes, and Douban data, and provides up-to-date ratings periodically.

![dags_update_ratings](https://user-images.githubusercontent.com/97599669/197456262-334bc1ef-70b9-4527-8202-7fe903250cbe.png)

## MySQL Schema
![01e500fca29c045d432b64f285f9c229](https://user-images.githubusercontent.com/97599669/197459064-554b15d9-12ca-4934-8a24-2b31555809f8.png)

## Server Structure
![DATA_STRUCTURE](https://user-images.githubusercontent.com/97599669/197459143-abb62380-db5e-46a7-8fb2-9a0f154b90a4.png)

## Features
### Dashboard for data pipeline monitoring
![dashboard](https://user-images.githubusercontent.com/97599669/197510719-04fdcc90-4db8-44b4-aac8-ea6c438d90f4.gif)


### Search movies with multiple conditions
![home_02](https://user-images.githubusercontent.com/97599669/197514795-233540b2-8af2-4f74-b9a9-03a2060a994e.gif)


### Show movie ratings from multiple web pages
![home_03](https://user-images.githubusercontent.com/97599669/197515568-07f43cd0-0cb9-4dff-94f0-9c926d40bea6.gif)

### Match Rotten tomatoes ID by the conditional filter
Create a conditional filter to filter the movie list from Rotten Tomatoes web search result, and match Rotten Tomatoes ID with IMDb ID.
#### The filtering feature:
* The string similarity ratio between two titles >= 0.5 
* The difference between the movie year <= 2
* The count of same actors between two movies >= 2




## Technologies
#### Data Pipeline
*  Airflow

#### Backend
*  Flask

#### Database
*  MongoDB
*  MySQL

#### Front-end
* HTML
* JavaScript
* CSS
* AJAX
* Plotly

#### Networking
* https
* SSL
* Domain Name system
* Nginx

#### Cloud Service
*  AWS EC2
*  AWS RDS
*  AWS S3

#### Web Crawler
* Selenium
* Beautifulsoup

#### Test
* unittest

#### Others
* Version Control: Git & GitHub
* Scrum: Trello

## Contact Me
Sing Rong newro9333@gmail.com