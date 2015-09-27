# audience-behavior-semantic-etl

The data pipeline describes Sparkline metrics driven ETL approach. Six rich data cubes full of advanced metrics were created from the last 60 days of raw event data. No SQL was written to build this data-pipeline. Data cubes were created based on business metrics design. 

#### Semantic Data Pipeline Design for Acme Fitness

An e-commerce company, Acme Fitness, is capturing user behavior data from user clicks on its website and from inside a mobile app. The event data is in semi-structured JSON format and is dumped on Amazon S3 in compressed JSON format every hour. 

Acme Fitness wanted to build an autonomous data pipeline to read raw event data from S3 on a daily frequency (frequency is programmable) and transform it to populate a rich data-warehouse consisting of many cubes to perform deep user-behavior analysis.

**Analytics Desired For Understanding Audience Behavior**

Acme is interested in understanding the:
- sessionization of data
- trend of events, sessions and users as well as slice and dice trends by various dimensions
- engagement and behavior metrics of users
- conversion rates by various segments including campaigns
- behavior of cohorts
- influence of various impact events and impact pages on conversion goals
- revenue attribution to impact events and pages

> **Data Pipeline Workflow**

![Image](img/SparklineApp-UserTrends.png?raw=true)
![Image](http://sparkline-beta.s3-website-us-east-1.amazonaws.com/images/step-o-data-pipeline-design.png)

> **Data Lake on S3 - Folders With Event Logs**

![Image](http://sparkline-beta.s3-website-us-east-1.amazonaws.com/images/s3-daily-buckets-ss.png)

> **Raw Event Data In Compressed JSON**

Event log is stored every hour on S3 in a daily bucket.
![Image](http://sparkline-beta.s3-website-us-east-1.amazonaws.com/images/raw-data-ss.png)

> **Review Event Sample Data and Schema**

Sample Data
```
[1f9611d5-e769-4e1e-8a97-7ef98be3b77e,null,client,[null,98.200.241.39,[analytics.js,2.10.0],
[/cart.htm,https://www.acmefitness.com/cart.htm,,Acme Fitness Shopping Basket - View Your Acme Shopping Basket,
https://www.acmefitness.com/cart.htm],Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) 
Chrome/44.0.2403.157 Safari/537.36],AddToBag,[],bb01b516-f797-4e65-8c53-cc484e665892,null,2015-09-01T00:00:35.659Z,
VhBYQcAjH5,[null,null,null,null,null,null,null,null,List([449.0,Charge,1], [0.0,GelSample_GWP,1], 
[0.0,SerumSample_GWP,1], [0.0,OBBFSample_GWP,1], [0.0,Surge_GWP,1], 
[0.0,Zero,1], [0.0,GGiftCard_GWP,1]),null,null,null,null,null,null,null,null],2015-09-01T00:00:33.020Z,
2015-09-01T00:00:35.662Z,2015-09-01T00:00:33.017Z,track,null,2]
```

> **Event Log Schema (Pt-I)**

```
|– anonymousId: string (nullable = true) 
|– category: string (nullable = true) 
|– channel: string (nullable = true) 
|– context: struct (nullable = true) 
| |– campaign: struct (nullable = true) 
| | |– content: string (nullable = true) 
| | |– medium: string (nullable = true) 
| | |– name: string (nullable = true) 
| | |– source: string (nullable = true) 
| | |– term: string (nullable = true) 
| |– ip: string (nullable = true) 
| |– library: struct (nullable = true) 
| | |– name: string (nullable = true) 
| | |– version: string (nullable = true) 
| |– page: struct (nullable = true) 
| | |– path: string (nullable = true) 
| | |– referrer: string (nullable = true) 
| | |– search: string (nullable = true) 
| | |– title: string (nullable = true) 
| | |– url: string (nullable = true) 
| |– userAgent: string (nullable = true) 
|– event: string (nullable = true) 
|– integrations: struct (nullable = true) 
|– messageId: string (nullable = true) 
|– name: string (nullable = true) 
|– originalTimestamp: string (nullable = true) 
|– projectId: string (nullable = true)
|– properties: struct (nullable = true) 
| |– couponcode: string (nullable = true) 
| |– currency: string (nullable = true) 
| |– discount: string (nullable = true) 
| |– emailId: string (nullable = true) 
| |– name: string (nullable = true) 
| |– orderId: string (nullable = true) 
| |– path: string (nullable = true) 
| |– product: string (nullable = true) 
| |– products: array (nullable = true) 
| | |– element: struct (containsNull = true) 
| | | |– price: string (nullable = true) 
| | | |– productname: string (nullable = true) 
| | | |– quantity: string (nullable = true) 
| |– referrer: string (nullable = true) 
| |– revenue: string (nullable = true) 
| |– search: string (nullable = true) 
| |– sessionId: string (nullable = true) 
| |– shipping: string (nullable = true) 
| |– tax: string (nullable = true) 
| |– title: string (nullable = true) 
| |– url: string (nullable = true) 
|– receivedAt: string (nullable = true) 
|– sentAt: string (nullable = true) 
|– timestamp: string (nullable = true) 
|– type: string (nullable = true) 
|– userId: string (nullable = true) 
|– version: long (nullable = true)
```

> **Step I - Transform Raw Events**

**Use Semantic ETL to enrich raw events and build Event Metrics cube**
Raw data shown above need to be now transformed. Sparkline's SemanticETL product is used for rapidly assembling ETL steps to transform this data. See the work-flow below to understand the sequence of steps that are applied to transform raw data.
Sparkline Semantic ETL has a rich library of functions (Spark and Hive UDFs) to transform user-agent, IP address and UTM information and expand it into number of columns. The library also includes data function to transform UTC or any other time format time-stamp to year, month, day, hour etc.
Here is a sample list of UDFs which come handy while building data-pipeline:
- Parse User Agent
- Parse UTM
- Parse IP Address and Geo-code using MaxMind
- Parse Date
- Flatten Array (Structs)
- Column functions such as drop, rename, remove nulls and conditional statements based
- Row functions such as filter, drop duplicate rows

![image](http://sparkline-beta.s3-website-us-east-1.amazonaws.com/images/step-i-etl.png)




