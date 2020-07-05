# floyo-ML-scala
Distributed ML for eCommerce platforms (recommendations, churn prediction, segmentation) written in Scala, using Spark MLlib, Elasticsearch (elastic4s), and AWS (AWS SDK)

## Concept:
1. Drop your transactional/sales data into S3
2. Trigger the Seed process 
    - e.g.: 
      - *AWS SNS trigger on S3*
      - *cron*
      - *SBT CLI*
3. Search/scroll written Elasticsearch data to automate some business process
    - e.g.:
      - *automate emails to key customer segments such as most loyal, small baskets, infrequent shoppers, etc.*
      - *recommend products intelligently on your eCommerce website*
      - *automate a push notification to customers who are at risk of churning with a special offer*

![Diagram](https://floyalty-ca.s3.ca-central-1.amazonaws.com/floyomlscala-diagram.svg)

| Feature                                                                                 | Progress   |
|-----------------------------------------------------------------------------------------|------------|
| Customer segmentation via K-Means++                                                     | 0.1        |
| Churn prediction via logistic regression                                                | In progress|
| Product recommendations via matrix factorization (collaborative filtering)              | 0.1        |
| Email automation                                                                        | To-do      |
| Elasticsearch integration                                                               | 0.1        |
| S3 integration                                                                          | 0.1        |
| Kibana dashboard & visualizations                                                       | To-do      |
