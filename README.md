# python-kafka-es-sink
A very simple sink that consume Kafka message and push it to Elasticsearch
----

## Required libraries
- Python 3.7
- elasticsearch 7.10.1
  - IMPORTANT: version >7.10 has Elasticsearch product check that fails on OpenDistro ES, like AWS OpenSearch
- kafka-python 
  - Tested with 2.0.1

```bash
pip3 install elasticsearch==7.10.1 kafka-python
```
