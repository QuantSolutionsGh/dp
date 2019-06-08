import sys
import codecs
import datetime
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import UserDefinedFunction
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


# Helper method to get hours between two timestamps
def generate_timestamp_series(start, stop):
    if not start:
        return []
    return [start + x for x in
            range(0, ((stop or start) - start) + 1, ONE_HOUR)]


# If there's a raw value for this column, returns "'VALUE' AS column_name"
def get_column_select(name):
    if name in raw_values:
        value = raw_values[name]['value']
        if raw_values[name]['type'] == 'string':
            value = "'" + value + "'"
        return '{} AS {}'.format(value, name)
    return name


# support UTF-8 when printing data
if sys.stdout.encoding != 'UTF-8':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout, 'strict')
if sys.stderr.encoding != 'UTF-8':
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr, 'strict')

# Get parameters
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 'input_database', 'output_connection',
                           'output_database', 'mappings_by_version'])
input_database = args['input_database']
output_connection = args['output_connection']
output_database = args['output_database']
mappings_by_version = json.loads(args['mappings_by_version'])

print(mappings_by_version)

# Start the job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Iterate over all versions
for version, mappings in mappings_by_version.items():
    column_mappings = []
    raw_values = {}
    # Parse column mappings and raw values
    for mapping in mappings:
        input = mapping['input']
        output = mapping['output']
        if input.get('columnName'):
            column_mappings.append((input.get('columnName'), input['type'],
                                    output.get('columnName'), output['type']))
        else:
            raw_values[output.get('columnName')] = {
                'value': input.get('value'),
                'type': output.get('type')
            }
    # Load logs from a specific version
    log_datasource = glueContext.create_dynamic_frame.from_catalog(
        database=input_database, table_name=version,
        transformation_ctx="log_datasource")
    # Apply column mappings
    raw_data_column_mapping = ApplyMapping.apply(frame=log_datasource,
                                                 mappings=column_mappings,
                                                 transformation_ctx="raw_data_column_mapping")
    # Make columns
    make_cols_resolve_choice = ResolveChoice.apply(
        frame=raw_data_column_mapping, choice="make_cols",
        transformation_ctx="make_cols_resolve_choice")
    # Drop null fields
    raw_data = DropNullFields.apply(frame=make_cols_resolve_choice,
                                    transformation_ctx="raw_data")

    # At this point we have a temporary table called "raw_data" with new log
    # entries
    raw_data.toDF().createOrReplaceTempView("raw_data")
    ONE_HOUR = 3600000
    # Partition this data by device and book, get next event timestamp,
    # duration and save as 'events' table
    event_query_string = """
  SELECT event_timestamp,
         {organization_code},
         {program_id},
         {project_id},
         {site_id},
         grade_id,
         device_id,
         book_id,
         event_type,
         next_timestamp,
         event_timestamp - (event_timestamp % {one_hour}) AS starting_hour,
         next_timestamp - (next_timestamp % {one_hour}) AS ending_hour,
         next_timestamp - event_timestamp AS event_duration
  FROM (SELECT
    *,
    LEAD(event_timestamp, 1) OVER (PARTITION BY device_id, book_id ORDER BY 
    event_timestamp) AS next_timestamp
  FROM raw_data
  ORDER BY device_id, event_timestamp) timestamps
  """.format(
        organization_code=get_column_select('organization_code'),
        program_id=get_column_select('program_id'),
        project_id=get_column_select('project_id'),
        site_id=get_column_select('site_id'),
        one_hour=ONE_HOUR)

    event_sql_df = spark.sql(event_query_string)

    event_sql_df.createOrReplaceTempView("events")

    event_sql_dyf = DynamicFrame.fromDF(event_sql_df, glueContext,
                                        "event_sql_dyf")

    # Get possible hours these events could fall into
    spark.udf.register("generate_timestamp_series", generate_timestamp_series,
                       ArrayType(LongType()))

    hour_sql_df = spark.sql(
        "SELECT DISTINCT(explode(generate_timestamp_series(starting_hour, "
        "ending_hour))) AS hour FROM events")

    hour_sql_df.createOrReplaceTempView("available_hours")

    hour_sql_dyf = DynamicFrame.fromDF(hour_sql_df, glueContext, "hour_sql_dyf")

    # Calculate book event aggregations (reading duration, number of events,
    # etc.).
    aggregation_query_string = """
  SELECT hour, organization_code, program_id, project_id, site_id, grade_id, 
  device_id, book_id, 
    SUM(duration) AS reading_duration, 
    SUM(book_loading) AS book_loading_events, 
    SUM(book_finished) AS  book_finished_events, 
    SUM(book_read) AS book_page_read_events 
  FROM (
    SELECT *, 
    CASE WHEN event_type = 'BookLoading' THEN 1 ELSE 0 END AS book_loading, 
    CASE WHEN event_type = 'BookFinished' THEN 1 ELSE 0 END AS book_finished, 
    CASE WHEN event_type = 'BookRead' THEN 1 ELSE 0 END AS book_read,
    CASE 
      WHEN event_type <> 'BookRead' 
        THEN 0
      WHEN event_timestamp >= hour AND next_timestamp <= (hour + {one_hour})
        THEN next_timestamp - event_timestamp
      WHEN event_timestamp <= hour AND next_timestamp >= (hour + {one_hour})
        THEN {one_hour}
      WHEN event_timestamp <= hour
        THEN next_timestamp - hour
      WHEN next_timestamp >= hour
        THEN (hour + {one_hour}) - event_timestamp
      ELSE 0
    END as duration
    FROM available_hours JOIN events ON hour >= starting_hour AND hour <= 
    ending_hour AND 
    (event_type = 'BookRead' OR event_type = 'BookLoading' OR event_type = 
    'BookFinished')
  ) book_event_aggregations 
  GROUP BY hour, organization_code, program_id, project_id, site_id, 
  grade_id, device_id, book_id
  """.format(one_hour=ONE_HOUR)

    # New aggregated rows - temporary table
    aggregation_sql_df = spark.sql(aggregation_query_string)

    aggregation_sql_df.createOrReplaceTempView("book_event_aggregations")

    aggregation_sql_dyf = DynamicFrame.fromDF(aggregation_sql_df, glueContext,
                                              "aggregation_sql_dyf")

    # Write the table (Postgres)
    temp_aggregation_datasink = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=aggregation_sql_dyf, catalog_connection=output_connection,
        connection_options={"dbtable": "temp_book_event_aggregations",
                            "database": output_database},
        transformation_ctx="temp_aggregation_datasink")

# Finish the job
job.commit()
