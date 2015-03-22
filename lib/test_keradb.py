import unittest
import json
import keradb


class KeraDBTestCase(unittest.TestCase):
  def setUp(self):
    self.keradb = keradb.KeraDB("testdata/test_index.csv", "")
    self.keradb.load_index()
    self.requests = DummyRequets()

  def test_should_load_index(self):
    self.assertEquals((0, 2800), self.keradb.index["a.V"])
    self.assertEquals((20406, 2520), self.keradb.index["m.m"])

  def test_should_get_record(self):
    self.assertEquals("Kidds App",
                      json.loads(self.keradb.get("k.a", requests=self.requests))["app_name"])

  def test_should_raise_error_on_invalid_key(self):
    self.assertRaises(keradb.RecordNotFound, self.keradb.get, "doesnt_exists", False,
                      self.requests)

  def test_should_not_raise_error_on_mismatched_key(self):
    self.assertTrue(len(self.keradb.get("a.V", False, self.requests)) > 0)

  def test_should_raise_error_on_mismatched_key(self):
    self.assertRaises(keradb.ValidationError, self.keradb.get, "a.V", True,
                      self.requests)

  def test_should_raise_error_on_invalid_json_response(self):
    self.requests.record = self.requests.record[:-1]
    self.assertRaises(keradb.ValidationError, self.keradb.get, "f.p", True,
                      self.requests)


class DummyRequets(object):
  def __init__(self):
    with open("testdata/sample_record.json") as f:
      self.record = f.read()

  def get(self, *args, **kwargs):
    return DummyResponse(text=self.record)


class DummyResponse(object):
  def __init__(self, text):
    self.text = text

  def json(self):
    return json.dumps(self.text)
