# floyo-ML-scala
Distributed ML for eCommerce platforms (recommendations, churn prediction, segmentation) written in Scala, using Spark MLlib, Kafka, Elasticsearch (elastic4s), and AWS

## Process:
1. Drop your transactional/sales data into S3 to train the models=
2. Trigger the training process 
    - e.g.: 
      - *AWS SNS trigger on S3*
      - *cron*
      - *SBT CLI*
3. Write new transactions to Kafka streams to make predictions in realtime
    - Persisted ML models are used to make predictions from transactions that are read from stream
4. Search/scroll written Elasticsearch data to automate some business process
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
| Read transactions from stream in realtime to make predictions                           | 0.1        |
