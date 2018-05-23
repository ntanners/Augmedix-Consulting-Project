# From Log to O(Log)* Nadav Tanners
* or better

## An Insight Data Engineering Consulting Project by Nadav Tanners

## Table of Contents
1. [Project Summary](README.md#project-summary)
2. [Company Description](README.md#company-description)
3. [Problem Description](README.md#problem-description)
4. [My Work](README.md#my-work)
    * [Proof-of-Concept Data Pipeline](README.md#proof-of-concept-data-pipeline)

1. [Introduction](README.md#introduction)

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

Moving forward, however, they are also interested in leveraging historical log data -- together with other non-tabular data (including JSON) -- to support predictive analytics of connectivity problems.  This use case may not be compatible with their current data management approach, because MySQL is not optimized for full-text search.  As their tables get larger, running any kind of analysis on the full tables in MySQL may become prohibitively slow.  To address this potential problem, I am proposing to move their admin log data to Elasticsearch.

Relative to MySQL, Elasticsearch offers a number of advantages for Augmedix's use case:  
1. Elasticsearch is optimized for full-text search through the use of a Lucene index, which allows users to search through full text in millions of records in fractions of a second.  
2. Elasticsearch is a distributed database, making it more scalable than MySQL.  If you need to add more space, it is simple to add additional nodes to your Elasticsearch cluster.  
3. Elasticsearch stores records as JSON documents, so it provides additional flexibility for storing the non-tabular data sources that Augmedix would like to incorporate into its predictive analytics.

### My Work

#### Proof-of-Concept Pipeline
To demonstrate the advantages of Elasticsearch over MySQL for Augmedix's needs, I built a proof-of-concept data pipeline.  I first downloaded a sample of Augmedix's admin log tables and imported them into my own MySQL database on Amazon RDS.  I then used Python and the Elasticsearch bulk API to import these tables into an Elasticsearch cluster on Amazon EC2 instances.  Because Elasticsearch does not support joins across different document types, I used a flattening query to join several MySQL tables into a single table before importing it.

Once I imported some admin log data into Elasticsearch, I was able to demonstrate its search performance advantage, achieving a reduction in search time of over 99% from about five and a half seconds to 27 miliseconds.  As these tables get larger -- reaching hundreds of millions or billions of rows -- this performance advantage will become more significant.

### Optimizing Indexing Speed
Ideally, however, Augmedix would not need to maintain two data storage systems for the same data.  If Elasticsearch can replace all of the functions currently performed by MySQL, then that will eliminate the need for MySQL storage of admin log data altogether.  Instead, Augmedix could use a messaging service such as Kafka to deliver admin logs straight from the source into Elasticsearch.

In order to 

### Data Description:
Augmedix is able to track multiple elements (but not all) of wifi connectivity, scribe connectivity, server health, bluetooth connectivity, stream state, actions taken by clinicians on their devices, actions taken by documentation specialists on the web applications etc. This data is generally addressed as “log data” from different tools, apps, systems and users. Streaming and sensitive data is persisted for a minimum of three days.

### Prior Work:
Initial tools to help create a single source of truth have been created (primarily SQL queries of various DBs, but not a unified view of the connectivity). This can be leveraged to quickly understand the various elements relevant to each type of data.

### Deliverable:
A unified source of data for investigations that can be utilized to rapidly resolve issues (predict the root cause) and if possible, predict probability of an issue.

### Implementation:
None. We’re open and excited to understand the tradeoffs of various technologies and which approach would be chosen to solve this problem. Our desire is to find a solution that can be integrated into our platform and maintained / developed further to help solve this problem over time, which might limit some too far out there ideas, but are interested to learn what is the cutting edge.
