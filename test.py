import copy
import json
import logging
import os
import time
from collections import OrderedDict
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from io import open


file_data = os.path.join("/home/work/data", "weird.txt")
data = open(file_data, 'rt', encoding="utf-8")
data.readline()
print(data)