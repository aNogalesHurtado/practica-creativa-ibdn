import sys, os, re
import datetime, iso8601
from cassandra.cluster import Cluster

_cassandra_session = None

def get_cassandra_session():
  global _cassandra_session
  if _cassandra_session is None:
    cluster = Cluster([os.environ.get('CASSANDRA_HOST', '127.0.0.1')])
    _cassandra_session = cluster.connect('agile_data_science')
  return _cassandra_session

def process_search(results):
  records = []
  total = 0
  if results['hits'] and results['hits']['hits']:
    total = results['hits']['total']
    hits = results['hits']['hits']
    for hit in hits:
      record = hit['_source']
      records.append(record)
  return records, total

def get_navigation_offsets(offset1, offset2, increment):
  offsets = {}
  offsets['Next'] = {'top_offset': offset2 + increment, 'bottom_offset': offset1 + increment}
  offsets['Previous'] = {'top_offset': max(offset2 - increment, 0), 'bottom_offset': max(offset1 - increment, 0)}
  return offsets

def strip_place(url):
  try:
    p = re.match('(.+)\?start=.+&end=.+', url).group(1)
  except AttributeError as e:
    return url
  return p

def get_flight_distance(client, origin, dest):
  session = get_cassandra_session()
  row = session.execute(
    "SELECT distance FROM origin_dest_distances WHERE origin=%s AND dest=%s",
    (origin, dest)
  ).one()
  return row.distance if row else 0

def get_regression_date_args(iso_date):
  dt = iso8601.parse_date(iso_date)
  day_of_year = dt.timetuple().tm_yday
  day_of_month = dt.day
  day_of_week = dt.weekday()
  return {
    "DayOfYear": day_of_year,
    "DayOfMonth": day_of_month,
    "DayOfWeek": day_of_week,
  }

def get_current_timestamp():
  iso_now = datetime.datetime.now().isoformat()
  return iso_now
