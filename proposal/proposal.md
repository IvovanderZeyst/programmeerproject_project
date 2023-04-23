# Table of Contents
- [Some answers to start with](#some-answers-to-start-with)
   - [The problem](#the-problem)
   - [Users](#users)
   - [Usage setting](#usage-setting)
   - [What does my web app do differently](#what-does-my-web-app-do-differently)
- [Sketches](#sketches)
- [Features](#features)
  - [Listed](#listed)
    - [Features](#features)
      - [Must haves](#must-haves)
      - [Nice to haves](#nice-to-haves)
- [Requirements](#requirements)
  - [Sources](#sources)
  - [Python Libraries](#python-libraries)
- [Expected challenge](#expected-challenge)


# Project proposal

## Some answers to start with

### The problem

Since Covid struck, the world of sport bikes (road bikes, mountain bikes, time trial bikes and the likes ) is in 
disarray. As a consumer, it's hard to find a bike, let alone the right bike. The below factors contribute to that issue.
- Production limitations
- Supply chain issues
- High demand, limited supply
- Hardly any bikes are being sold online
- Most reviews are incentivized by bike brands and therefore unreliable

### Users

Anyone looking for a bike and wanting to know what is the right bike for them, based on specifications that are valued 
by cyclists.

### Usage setting

Individual behind a desktop or laptop

### What does my web app do differently

There is a website called 99spokes that contains a lot of data. It offers only limited comparison and filter options 
and while it serves a comparison purpose, the filter options are very limited. Strangely enough they offer an API 
with way more extensive data than they present on their own website. Based on the data they make available, I will build
a tool in the form of a web app that takes a more orientation and purchase based approach.

## Sketches

[Find the sketches here](https://www.figma.com/file/ejfedIdjG2nnxIFH3co1YA?embed_host=share&kind=&node-id=0%3A1&t=nUI5HUZSCfdriOfR-1) 

## Features

### Listed

- User registration
- User login
- API data extraction 99 spokes
- Data transformation 99 spokes data
- Database
- Data model
- Build filter page
- Bookmark bike
- Bookmark filter settings
- Implement favorite bike page
- Implement saved filter page
- Toggle view on find bike, save bike and save filter view
- Buy bike button  for different outlets that executes search on the selected site to try and find one for sale

#### Must haves

- User registration
- User login
- API data extraction 99 spokes
- Data transformation 99 spokes data
- Database
- Data model
- Build filter page
- Bookmark bike
- Bookmark filter settings
- Implement favorite bike page
- Implement saved filter page

#### Nice to haves

- Toggle view on find bike, save bike and save filter view
- Buy bike button  for different outlets that executes search on the selected site to try and find one for sale

## Requirements

### Sources

- [99spokes websites](https://99spokes.com/en-EU)
- [99spokes API documentation](https://api.99spokes.com/docs)

API key already received

### Python libraries

- SQLalchemy
- Flask-Mail
- Flask-Login
- Flask-Admin
- Flask-User
- Flask-Security
- Flask-WTF
- Pandas 2.0
- Requests

## Expected challenge

I expect the ETL work to be challenging