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

> **Event Log Schema**

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

> **Event Metrics Cube Schema**

```
|– page_name: string (nullable = true) 
|– sd_product_quantity: string (nullable = true) 
|– event_path: string (nullable = true) 
|– campaign_content: string (nullable = true) 
|– event_param: string (nullable = true) 
|– channel: string (nullable = true) 
|– user_agent: string (nullable = true) 
|– utc_time: string (nullable = true) 
|– campaign_source: string (nullable = true) 
|– event_name: string (nullable = true) 
|– project_id: string (nullable = true) 
|– order_revenue: string (nullable = true) 
|– ordertax: string (nullable = true) 
|– currency: string (nullable = true) 
|– campaign_name: string (nullable = true) 
|– order_id: string (nullable = true) 
|– campaign_term: string (nullable = true) 
|– page_referrer: string (nullable = true) 
|– event_referrer: string (nullable = true) 
|– order_discount: string (nullable = true) 
|– campaign_medium: string (nullable = true) 
|– sd_product_price: string (nullable = true) 
|– sd_product_name: string (nullable = true) 
|– event_id: string (nullable = true) 
|– page_params: string (nullable = true) 
|– event_url: string (nullable = true)
|– event_title: string (nullable = true) 
|– page_url: string (nullable = true) 
|– received_at: string (nullable = true) 
|– ip_address: string (nullable = true) 
|– page_path: string (nullable = true) 
|– segment_sent_at: string (nullable = true) 
|– product: string (nullable = true) 
|– order_coupon_code: string (nullable = true) 
|– segment_user_id: string (nullable = true) 
|– type: string (nullable = true) 
|– user_id: string (nullable = true) 
|– sd_day_name: integer (nullable = true) 
|– sd_hour_of_day: integer (nullable = true) 
|– sd_country_code: string (nullable = true) 
|– sd_device_family: string (nullable = true) 
|– sd_continent: string (nullable = true) 
|– sd_longitude: double (nullable = true) 
|– sd_city: string (nullable = true) 
|– sd_browser_version_patch: string (nullable = true) 
|– sd_device_os: string (nullable = true) 
|– sd_region: string (nullable = true) 
|– sd_latitude: double (nullable = true) 
|– sd_device_os_version_patch: string (nullable = true) 
|– sd_device_os_version_major: string (nullable = true) 
|– sd_country_name: string (nullable = true) 
|– sd_postal_code: string (nullable = true)
|– sd_browser_version_major: string (nullable = true) 
|– sd_device_os_patch_minor: string (nullable = true) 
|– sd_browser: string (nullable = true) 
|– sd_browser_version_minor: string (nullable = true) 
|– sd_device_version_minor: string (nullable = true) 
|– sd_host: string (nullable = true) 
|– sd_path: string (nullable = true) 
|– sd_query: string (nullable = true) 
|– sd_campaign: string (nullable = true) 
|– sd_source_o: string (nullable = true) 
|– sd_medium_o: string (nullable = true) 
|– sd_content: string (nullable = true) 
|– sd_term: string (nullable = true) 
|– sd_product_category: string (nullable = true) 
|– sd_is_revenue_event: integer (nullable = true) 
|– sd_is_cart_event: integer (nullable = true) 
|– sd_is_video_event: integer (nullable = true) 
|– sd_product_revenue: double (nullable = true) 
|– sd_source: string (nullable = true) 
|– sd_medium: string (nullable = true) 
|– sd_previous_event_timestamp: string (nullable = true) 
|– sd_year: integer (nullable = true) 
|– sd_month: integer (nullable = true) 
|– sd_day: integer (nullable = true) 
|– sd_week: integer (nullable = true) 
|– sd_hour: integer (nullable = true)
```

> **Step II - Sessionize Data**

**Compute Session Metrics**
In this stage of the data pipeline, the event metrics cube is transformed and a session metrics cube is built. Session metrics such as landing screen, exit screen, session rank, start time and end time etc are computed and stored in session metrics cube.

Here is a sample of session metrics which get computed every day and get added to session metrics cube:
- session_rank
- session_start_time
- session_end_time
- session_duration
- session_landing_screen
- session_exit_screen
- session_event_count
- session_event_path
- session_page_path
- session_page_name
- session_page_referrer
- session_product_list
- session_product_quantity
- session_revenue
- is_revenue_session
- is_cart_session
- is_video_session
- is_bounce_session
- avg_time_per_event

![image] (http://sparkline-beta.s3-website-us-east-1.amazonaws.com/images/step-ii-etl.png)

> **Session Metrics Cube Schema**

New columns added to event metrics cube and the grain of the cube is at session level.
```
|– sd_previous_event_timestamp: string (nullable = true) 
|– sd_session_id: string (nullable = true) 
|– sd_session_rank: integer (nullable = true) 
|– sd_session_start_time: long (nullable = true) 
|– sd_session_end_time: long (nullable = true) 
|– sd_session_event_count: long (nullable = true) 
|– sd_session_landing_screen: string (nullable = true) 
|– sd_session_exit_screen: string (nullable = true) 
|– sd_session_page_referrer: string (nullable = true) 
|– sd_session_product_list: array (nullable = true) 
|– sd_session_product_quantity: double (nullable = true) 
|– sd_session_revenue: double (nullable = true) 
|– sd_is_revenue_session: integer (nullable = true) 
|– sd_is_cart_session: integer (nullable = true) 
|– sd_is_video_session: integer (nullable = true) 
|– sd_session_duration: double (nullable = true) 
|– sd_is_bounce_session: integer (nullable = true) 
|– sd_avg_time_per_event: double (nullable = true)
```

> **Step III - Aggregate Data at User Level**

**Compute User Metrics Cube and Rank Users**

In this stage of the data pipeline, the session metrics cube is transformed and a user metrics cube is built. User metrics such as daily revenue, number of sessions, number of revenue sessions, first seen at, rank by different dimensions are computed and stored in user metrics cube.

Here is a sample of user metrics which get computed every day and get added to user metrics cube: 

Metrics columns
- daily_revenue
- daily_product_quantity
- session_count
- daily_time_spent
- avg_session_time
- event_count
- avg_num_events_per_session
- last_session_end_time
- earliest_session_start_time
- num_revenue_session
- num_video_session
- num_cart_session

Ranking columns
- rank_by_event_count
- rank_by_session_count
- rank_by_time_spent

Stage II ETL - determine if new user
- first_seen_at
- is_new_user

![image](http://sparkline-beta.s3-website-us-east-1.amazonaws.com/images/step-iii-etl.png)

**User Metrics Cube Schema**

New columns added to user metrics cube and the grain of the cube is at user level.
```
|– sd_daily_revenue: double (nullable = true) 
|– sd_daily_product_quantity: double (nullable = true) 
|– sd_session_count: long (nullable = true) 
|– sd_time_spent: double (nullable = true) 
|– sd_avg_session_time: double (nullable = true) 
|– sd_event_count: long (nullable = true) 
|– sd_avg_num_events_per_session: double (nullable = true) 
|– sd_last_session_end_time: long (nullable = true) 
|– sd_earliest_session_start_time: long (nullable = true) 
|– sd_num_revenue_session: long (nullable = true) 
|– sd_num_cart_session: long (nullable = true) 
|– sd_num_video_session: long (nullable = true) 
|– sd_rank_by_event_count: integer (nullable = true) 
|– sd_rank_by_session_count: integer (nullable = true) 
|– sd_rank_by_time_spent: integer (nullable = true) 
|– sd_first_seen_at: long (nullable = true) 
|– sd_is_new_user: integer (nullable = false) 
|– sd_is_revenue_user: integer (nullable = false)
```

> **Step IV - Conversion Rate & Cohort Analysis Computations**

**Tag Users and Extract Key Dimensions to Build Conversion Cube**

In this stage of the data pipeline, the user metrics cube is transformed and a conversion rate cube is built. User metrics such as is_revenue_user and all_users are computed and stored along with other key dimension for conversion analysis.

Conversion cube schema
```
|– campaign_name: string (nullable = true) 
|– campaign_content: string (nullable = true) 
|– campaign_medium: string (nullable = true) 
|– campaign_source: string (nullable = true) 
|– campaign_term: string (nullable = true) 
|– sd_host: string (nullable = true) 
|– sd_path: string (nullable = true) 
|– sd_campaign: string (nullable = true) 
|– sd_source: string (nullable = true) 
|– sd_medium: string (nullable = true) 
|– sd_content: string (nullable = true) 
|– sd_term: string (nullable = true) 
|– utc_time: string (nullable = true) 
|– channel: string (nullable = true) 
|– ip_address: string (nullable = true) 
|– sd_browser: string (nullable = true) 
|– sd_device_os: string (nullable = true) 
|– sd_device_family: string (nullable = true) 
|– sd_country_code: string (nullable = true) 
|– sd_country_name: string (nullable = true) 
|– sd_region: string (nullable = true) 
|– sd_city: string (nullable = true) 
|– sd_latitude: double (nullable = true) 
|– sd_longitude: double (nullable = true) 
|– sd_postal_code: string (nullable = true) 
|– sd_continent: string (nullable = true) 
|– sd_rev_user: long (nullable = true) 
|– sd_all_user: long (nullable = true) 
|– sd_year: integer (nullable = true) 
|– sd_month: integer (nullable = true) 
|– sd_day: integer (nullable = true) 
|– sd_week: integer (nullable = true)
```
![image](http://sparkline-beta.s3-website-us-east-1.amazonaws.com/images/step-iv-etl.png)

> **Impact Analysis and Attribution Computations**

**Associate Impact Events and Pages to Goals and Compute Metrics**
In this complex stage of the data pipeline, the session cube is used to extract daily goal events and impact events over a look-back period. Look-back period is defined as a period over which users have been exposed to various impact events and pages to influence their behavior and drive more conversion. Impact events are then associated with goal events to rank impact events, pages and drive attribution analysis.

Following metrics are computed which help marketers in ranking impact events and their effectiveness using the influence on users, revenues etc.

- days_to_goal
- sessions_to_goal
- time_spent_on_site_to_goal
- revenue_attributed_to_impact_event
- impact_event_group_rank (used for last-touch attribution)

![image](http://sparkline-beta.s3-website-us-east-1.amazonaws.com/images/step-v-etl.png)

> **Mission Accomplished**

Using Sparkline's SemanticETL, AcmeBeauty transformed the raw events log to a set of rich cubes for query and analytics in just 5 steps and 2-stage ETL. These cubes are:
- Event Metrics
- Session Metrics
- User Metrics
- Conversion Rate and Cohort Analysis
- Goals-Impact Analysis and Attribution

In summary, raw event logs of any structure / schema can be very quickly transformed to rich cubes using business metrics which matter to business users and decision makers. No SQL is required to create an autonomous data-pipeline.
