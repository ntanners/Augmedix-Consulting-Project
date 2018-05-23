# From Log to O(Log)* Nadav Tanners
\* or better

## An Insight Data Engineering Consulting Project by Nadav Tanners

## Table of Contents
1. [Project Summary](README.md#project-summary)
2. [Company Description](README.md#company-description)
3. [Problem Description](README.md#problem-description)
4. [My Work](README.md#my-work)
    * [Proof-of-Concept Data Pipeline](README.md#proof-of-concept-pipeline)
    * [Optimizing Indexing Speed](README.md#optimizing-indexing-speed)
        * [Determining the Optimal Batch Size](README.md#determining-the-optimalbatch-size)
        * [Implementing Parallel API Calls](README.md#implementing-parallel-api-calls)
    * [Optimization Results](README.md#optimization-results)
5. [References](README.md#references)

### Project Summary:
I worked on a consulting project for a company called Augmedix, which is based out of San Francisco.  For my project, I focused on the problem that Augmedix stores log data in a MySL database, which is not optimized for full-text search.  I proposed moving Augmedix's log data from a MySQL database into Elasticsearch in order to take advantage of Elasticsearch's full-text search performance advantage in large datasets.  I constructed a proof-of-concept data pipeline that used Python and the Elasticsearch Bulk API to move data from MySQL to Elasticsearch.  I also optimized use of the Bulk API to import 100,000 records in less than seven seconds, demonstrating that ingestion into Elasticsearch can be fast enough to support accessing log data in real time. 

### Company Description:
Augmedix provides a service to doctors that lets them spend more time focusing on their patients and less time
staring at a computer screen.  They do this by giving doctors Google Glass, which records information from the patient visit and 
sends it to a remote scribe, who then fills out the patient note in the electronic medical record.  The doctor can then review
the note once the visit is complete.  As with any service based on IOT, connectivity is very important.

### Problem Description:
Augmedix tracks connectivity issues in admin log tables stored in a MySQL database, and I've shown an example of some of the data
in these tables.  Their largest such table is less than 20 million rows, but Augmedix is a young and growing company, so these tables
can be expected to grow much larger.  Augmedix currently uses admin log data to support real-time diagnosis, where they can track down the source of connectivity problems as they arise.  For this use case, they only need to query recent log data, so scale is less of a constraint.  

#### Figure 1. A Sample of Data from Augmedix's Admin Log Tables
![Admin Log Data Sample](https://github.com/ntanners/Augmedix-Consulting-Project/blob/master/Presentation_Materials/logDataExample.png)

Moving forward, however, they are also interested in leveraging historical log data -- together with other non-tabular data (including JSON) -- to support predictive analytics of connectivity problems.  This use case may not be compatible with their current data management approach, because MySQL is not optimized for full-text search.  As their tables get larger, running any kind of analysis on the full tables in MySQL may become prohibitively slow.  To address this potential problem, I am proposing to move their admin log data to Elasticsearch.

Relative to MySQL, Elasticsearch offers a number of advantages for Augmedix's use case:  
1. Elasticsearch is optimized for full-text search through the use of a Lucene index, which allows users to search through full text in millions of records in fractions of a second.  
2. Elasticsearch is a distributed database, making it more scalable than MySQL.  If you need to add more space, it is simple to add additional nodes to your Elasticsearch cluster.  
3. Elasticsearch stores records as JSON documents, so it provides additional flexibility for storing the non-tabular data sources that Augmedix would like to incorporate into its predictive analytics.

### My Work

#### Proof-of-Concept Pipeline
To demonstrate the advantages of Elasticsearch over MySQL for Augmedix's needs, I built a proof-of-concept data pipeline.  I first downloaded a sample of Augmedix's admin log tables and imported them into my own MySQL database on Amazon RDS.  I then used Python and the Elasticsearch bulk API to import these tables into an Elasticsearch cluster on Amazon EC2 instances.  Because Elasticsearch does not support joins across different document types, I used a flattening query to join several MySQL tables into a single table before importing it.

Once I imported some admin log data into Elasticsearch, I was able to demonstrate its search performance advantage, achieving a reduction in search time of over 99% from about 5.6 seconds to 27 miliseconds.  As these tables get larger -- reaching hundreds of millions or billions of rows -- this performance advantage will become more significant.

#### Optimizing Indexing Speed
The proof-of-concept pipeline demonstrates that Elasticsearch can outperform MySQL for batch analysis of large log tables.  Ideally, however, Augmedix would not need to maintain two data storage systems for the same data.  If Elasticsearch can replace all of the functions currently performed by MySQL -- including the real-time diagnostic function currently served by MySQL, then that will eliminate the need for MySQL storage of admin log data altogether.  Instead, Augmedix could use a messaging service such as Kafka to deliver admin logs straight from the source into Elasticsearch.

In order to show that Elasticsearch can also meet Augmedix's real-time diagnostic needs, I need to demonstrate that indexing records into Elasticsearch will not prove to be a bottleneck.  Indexing records into Elasticsearch can be slow, because the database generates a lucene index for each document as it is indexed.  I set a target of 7 seconds, because that's the slowest query that Augmedix runs in its real-time diagnostic tool.  

##### Determining the Optimal Batch Size
To optimize indexing speed, I first identified the optimal batch size for the data I was working with.  The Elasticsearch bulk API will increase indexing efficiency with larger batch sizes, but only up to a point, and that point will vary by the size of the data in the tables you want to index.  For the data I'm working with, the optimal batch size was around 6,000 rows.

#### Figure 2. Determining the Optimal Elasticsearch Bulk API Batch Size for Augmedix's Log Data 
![batchsizegraph](https://github.com/ntanners/Augmedix-Consulting-Project/blob/master/Presentation_Materials/batchSizeGraph.png)

##### Implementing Parallel API Calls
After identifying the optimal batch size, I implemented parallel calls to the Elasticsearch bulk API.  With parallel API calls, it is possible to overload your Elasticsearch cluster by making too many calls, but I did not reach that point with the number of workers I used.  Instead, I discovered that indexing speeds were much higher as long as the batch size and the number of workers together formed a factor of the total table size.  

#### Figure 3. Elasticsearch Indexing Speeds for 100,000 Records Using a Batch Size of 5,000 Records
![workernumbergraph](https://github.com/ntanners/Augmedix-Consulting-Project/blob/master/Presentation_Materials/workerNumberGraph.png)

Accordingly, importing 100,000 records with a batch size of 5,000 records was fastest using 10 or 20 workers.  This suggests that there is an overhead associated with each API call, so you maximize indexing speed by making sure that each API call has the optimal batch size. 

#### Optimization Results
After identifying the optimal batch size and implementing parallel API calls, I was able to index 100,000 rows in under 7 seconds, with the Elasticsearch API accounting for less than half of that total.  Whether or not Augmedix will be able to migrate all of their admin log data will depend on a number of factors, but it does not appear that slow indexing speed into Elasticsearch should be a constraint.

### References
* Handling relationships in Elasticsearch: [link](https://www.elastic.co/guide/en/elasticsearch/guide/current/relations.html)
* Optimizing indexing speed into Elasticsearch: [link](https://www.elastic.co/guide/en/elasticsearch/reference/master/tune-for-indexing-speed.html)
