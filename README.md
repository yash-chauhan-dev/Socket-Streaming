# Real-time Data Streaming with TCP Socket, Apache Spark, OpenAI LLM, Kafka, and Elasticsearch

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Learning Outcome](#learning-outcome)
- [Technologies](#technologies)
- [Getting Started](#getting-started)

## Introduction

Welcome to a comprehensive guide on building a robust end-to-end data engineering pipeline utilizing TCP/IP Socket, Apache Spark, OpenAI LLM, Kafka, and Elasticsearch. This project covers each stage of the pipeline, starting from data acquisition, processing, integrating sentiment analysis with ChatGPT, streaming to a Kafka topic, and connecting to Elasticsearch.

## System Architecture
![System_architecture.png](System_architecture.png)

The project is designed with the following key components:

- **Data Source**: Utilizes the `yelp.com` dataset for our pipeline.
- **TCP/IP Socket**: Streams data over the network in chunks.
- **Apache Spark**: Handles data processing with its master and worker nodes.
- **Confluent Kafka**: Deploys a cloud-based Kafka cluster.
- **Control Center and Schema Registry**: Facilitates monitoring and schema management of Kafka streams.
- **Kafka Connect**: Establishes the connection to Elasticsearch.
- **Elasticsearch**: Manages indexing and querying of data.

## Learning Outcome

By engaging with this project, you will gain expertise in the following areas:

- Setting up a data pipeline using TCP/IP.
- Real-time data streaming with Apache Kafka.
- Data processing techniques with Apache Spark.
- Real-time sentiment analysis with OpenAI ChatGPT.
- Synchronizing data from Kafka to Elasticsearch.
- Indexing and querying data on Elasticsearch.

## Technologies

This project leverages the following technologies:

- Python
- TCP/IP
- Confluent Kafka
- Apache Spark
- Docker
- Elasticsearch

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/yash-chauhan-dev/Socket-Streaming.git
    ```

2. Navigate to the project directory:
    ```bash
    cd Socket-Streaming
    ```

3. Run Docker Compose to spin up the Spark cluster:
    ```bash
    docker-compose up
    ```

Feel free to explore the detailed instructions and code within the repository to dive into the intricacies of this real-time data streaming project.