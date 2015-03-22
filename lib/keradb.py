import requests
import csv
import sys
import json


class KeraDB(object):
  def __init__(self, index_file, db_url):
    self.index_file = index_file
    self.db_url = db_url
    self.index_key_name = None
    self.index = None

  def load_index(self):
    with open(self.index_file) as csv_file:
      index_reader = csv.reader(csv_file, delimiter=",", quotechar="\"", escapechar="\\")
      self.index_key_name = next(index_reader)[0]
      self.index = dict(map(lambda row: (row[0], (int(row[1]), int(row[2]))), index_reader))

  def get(self, key, validate=False, requests=requests):
    if key not in self.index:
      raise RecordNotFound("%s key doesn't exist!")
    offset, size = self.index[key]
    request_headers = {"Range": "bytes=%d-%d" % (offset, offset + size - 1)}
    response = requests.get(self.db_url, headers=request_headers)
    if validate:
      self._validate(response, key)
    return response.text

  def _validate(self, response, requested_key):
    try:
      record = response.json()
      key = record[self.index_key_name]
    except:
      e = sys.exc_info()[0]
      raise ValidationError(e)
    if key != requested_key:
      raise ValidationError("Requested record id and returned record id doesn't match.\n"
                            "Requested: %s Got: %s" % (requested_key, key))


class ValidationError(Exception):
  pass


class RecordNotFound(Exception):
  pass


def create_index(index_key, db_filename, index_filename):
  offset = 0
  with open(index_filename, "wb+") as csv_file:
    index_writer = csv.writer(csv_file, delimiter=",", quotechar="\"", escapechar="\\")
    with open(db_filename) as db:
      index_writer.writerow([index_key, "offset", "size"])
      for line in db:
        record = json.loads(line)
        index_writer.writerow([record[index_key], offset, len(line)])
        offset += len(line)
