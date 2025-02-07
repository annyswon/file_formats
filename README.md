# CSV to Avro Converter

This repository contains a Python script that converts CSV files into Avro files. The script is designed to be easy to use and provides an efficient way to transform CSV data into a compact, schema-driven binary format suitable for large-scale data processing and storage.

## What is Avro?

Avro is a data serialization framework developed within the Apache Hadoop project. It is designed for:

- **Efficient Serialization:** Avro uses a compact binary format that reduces storage space and increases processing speed.
- **Schema-Driven Data Exchange:** Data is stored with its schema (defined in JSON), which ensures that data readers and writers agree on the data structure. This facilitates schema evolution and backward/forward compatibility.
- **Interoperability:** Avro is language-agnostic and integrates well with many big data tools and platforms, such as Apache Kafka, Apache Spark, and Hadoop.
- **Dynamic Typing:** The schema provides flexibility, allowing changes to data formats without breaking existing applications.

## Requirments

You have to install avro via pip for running this script

```bash
pip install avro
```

## Running

This script expects for .csv file in CSV_FILE_PATH and outputing .avro file with name in AVRO_NAME variable, also it takes in assumption about data types inside csv file for generating avro schema so there is no need for any additional info

[preview of avro file](https://github.com/annyswon/file_formats/blob/main/assets/photo_2025-02-07_8.22.05PM.jpeg)
