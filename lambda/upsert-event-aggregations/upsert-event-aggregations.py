import psycopg2
import os
from datetime import datetime, timedelta
from jwt import encode
import requests

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS book_event_aggregations
(
  hour bigint,
  organization_code varchar(10),
  program_id integer,
  project_id integer,
  site_id integer,
  grade_id integer,
  device_id integer,
  book_id varchar(50),
  book_title varchar(100),
  read_level_id integer,
  category_ids integer[],
  reading_duration bigint,
  book_loading_events bigint,
  book_finished_events bigint,
  book_page_read_events bigint
);
CREATE TABLE IF NOT EXISTS books
(
  id varchar(50),
  title varchar(100),
  read_level_id integer,
  category_ids integer[]
);
CREATE OR REPLACE FUNCTION execute_if_constraint_not_exists (
    tab_name text, constr_name text, sql text
) 
RETURNS VOID AS
$$
BEGIN
    IF NOT EXISTS (SELECT constraint_name 
                   FROM information_schema.constraint_column_usage 
                   WHERE table_name = tab_name AND constraint_name = constr_name) THEN
        execute sql;
    END IF;
END;
$$ LANGUAGE 'plpgsql';

SELECT execute_if_constraint_not_exists(
'book_event_aggregations',
'upsert_uniqueness',
'ALTER TABLE book_event_aggregations ADD CONSTRAINT upsert_uniqueness UNIQUE 
(hour, organization_code, program_id, project_id, site_id, grade_id, device_id, book_id);'
);
"""

SELECT_BOOK_IDS_SQL = """
SELECT DISTINCT book_id 
FROM temp_book_event_aggregations
WHERE NOT EXISTS (
  SELECT
  FROM books
  WHERE id = book_id
)
"""

UPSERT_SQL = """
INSERT INTO book_event_aggregations (
         hour, organization_code, program_id, project_id, site_id, grade_id, device_id, book_id, book_title, read_level_id, category_ids, reading_duration, book_loading_events, book_finished_events, book_page_read_events)
    SELECT hour, organization_code, program_id, project_id, site_id, grade_id, device_id, book_id, books.title, books.read_level_id, books.category_ids, reading_duration, book_loading_events, book_finished_events, book_page_read_events
    FROM temp_book_event_aggregations
    JOIN books ON book_id = books.id
    ON CONFLICT ON CONSTRAINT upsert_uniqueness DO UPDATE
    SET reading_duration = book_event_aggregations.reading_duration + EXCLUDED.reading_duration,
    book_loading_events = book_event_aggregations.book_loading_events + EXCLUDED.book_loading_events,
    book_finished_events = book_event_aggregations.book_finished_events + EXCLUDED.book_finished_events,
    book_page_read_events = book_event_aggregations.book_page_read_events + EXCLUDED.book_page_read_events;

TRUNCATE TABLE temp_book_event_aggregations;
"""

def chunks(l, n):
  for i in range(0, len(l), n):
    yield l[i:i + n]

def initialize_connection():
  return psycopg2.connect(user = os.environ['PG_USER'],
                  password = os.environ['PG_PASSWORD'],
                  host = os.environ['PG_HOST'],
                  port = os.environ['PG_PORT'],
                  database = os.environ['PG_DBNAME'])

def select_book_ids(cursor):
  cursor.execute(SELECT_BOOK_IDS_SQL)
  return [r[0] for r in cursor.fetchall()]

def get_api_token():
  JWT_SECRET = os.environ['TOKEN_JWT_SECRET']
  AUTH0_AUDIENCE = os.environ['TOKEN_AUTH0_AUDIENCE']

  payload = {
      'email': os.environ['TOKEN_EMAIL'],
      'iat': datetime.utcnow(),
      'exp': datetime.now() + timedelta(days=1),
      'name': os.environ['TOKEN_NAME'],
      'iss': os.environ['TOKEN_ISS'],
      'aud': AUTH0_AUDIENCE,
      os.environ['TOKEN_URL']: {
          'permissions': os.environ['TOKEN_PERMISSIONS'],
      }
  }
  return 'Bearer ' + str(encode(payload, JWT_SECRET))

def fetch_books(token, ids):
  BOOKS_API_URL = os.environ['BOOKS_API_URL']
  books = []
  for ids_chunk in chunks(ids, int(os.environ['BOOKS_API_PAGE_SIZE'])):
    url = BOOKS_API_URL + '/admin/books'
    params = {'uuid': ids_chunk}

    r = requests.get(url, params=params, headers={'Authorization': token})
    print(r.status_code)
    response = r.json()
    books.extend(response['books'])
  print(len(books))
  return(books)

def insert_books(token, cursor, books):
  book_tuples = []
  for book in books:
    read_level = book['readlevel']
    categories = [str(category.get('id')) for category in book['categories']]
    book_tuples.append((
      book['uuid'],
      book['title'],
      read_level.get('id'),
      '{' + ','.join(categories) + '}',
    ))
  args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s)", x).decode("utf-8") for x in book_tuples)
  cursor.execute("INSERT INTO books VALUES " + args_str) 

def upsert():
    connection = initialize_connection()
    cursor = connection.cursor()
    # Create tables
    cursor.execute(CREATE_SQL)
    print(cursor.statusmessage)

    # Select book ids
    book_ids = select_book_ids(cursor)
    print(cursor.statusmessage)

    # Get token
    token = get_api_token()

    # Fetch and insert books
    books = fetch_books(token, book_ids)
    if books:
      insert_books(token, cursor, books)
      print(cursor.statusmessage)

    # Upsert
    cursor.execute(UPSERT_SQL)
    print(cursor.statusmessage)

    connection.commit()
    cursor.close()

def lambda_handler(event, context):
  state = event.get('state', '')
  if state == 'SUCCEEDED':
    print('Event succeeded, starting the upsert')
    upsert()
  else:
    print('Event state: ' + state)
