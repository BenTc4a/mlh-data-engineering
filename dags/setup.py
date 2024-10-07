from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

# define default for DAGS
default_args = {
    'owner': 'tc4a',
    'start_date': datetime(2023, 11, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# create dags instance
dag = DAG(
    'tc4a_data_visualization',
    default_args=default_args,
    description='An Airflow DAG for tc4a Postgres to BigQuery',
    schedule_interval='@daily',
    catchup=False
)

# Bigquery config parameters
BQ_CON_ID = "gcp_connection"
BQ_PROJECT = "visualization-app-404406"
BQ_DATASET = "tc4a"
BQ_TABLE1 = "attendance"
BQ_TABLE2 = "bq_courses"
BQ_TABLE3 = "bq_training"
BQ_TABLE4 = "bq_total_training_hours"
BQ_TABLE5 = "bq_organizations"
BQ_TABLE6 = "bq_professions"
BQ_TABLE7 = "bq_course_enrollments"
BQ_TABLE8 = "bq_pskenya"
BQ_TABLE9 = "bq_NGO"
BQ_TABLE10 = "bq_Hospitals"
BQ_TABLE11 = "bq_Pharmas"
BQ_TABLE12 = "bq_Associations"
BQ_TABLE13 = "bq_KMA_data"
BQ_BUCKET = 'tc4a-backet'

# Postgres Config variables
PG_CON_ID = "postgres_default"
PG_SCHEMA = "public"

# Define tables from postgresql db
PG_TABLE1 = "lecture_watch_times"
PG_TABLE2 = "courses"
PG_TABLE3 = "lectures"
PG_TABLE4 = "organizations"
PG_TABLE5 = "professions"
PG_TABLE6 = "course_enrollments"
PG_TABLE7 = "accounts"
PG_TABLE8 = "stream_zoomreports"
PG_TABLE9 = "stream_attendee"

# events, enrolments, attendance

# Define Json files stored in the GCP bucket
JSON_FILENAME1 = 'attendance_data_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME2 = 'courses_data_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME3 = 'training_data_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME4 = 'organization_hours_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME5 = 'organizations_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME6 = 'profession_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME7 = 'course_enrollments_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME8 = 'pskenya_report_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME9 = 'ngos_report_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME10 = 'hospitals_report_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME11 = 'pharmas_report_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME12 = 'associations_report_' + datetime.now().strftime('%Y-%m-%d') + '.json'
JSON_FILENAME13 = 'kma_report_' + datetime.now().strftime('%Y-%m-%d') + '.json'

# Print project, dataset, and table information for debugging
print(f"Project: {BQ_PROJECT}")
print(f"Dataset: {BQ_DATASET}")
print(f"Table: {BQ_TABLE1}")

# Data Extraction
# Task to transfer data from postgres to Google cloud platform
# postgres_pskenya_report_to_gcs = PostgresToGCSOperator(
#     task_id='postgres_pskenya_report_to_gcs',
#     sql="""
#         SELECT
#             webinar_id AS webinar_id,
#             eu.specializations_id AS health_areas,
#             ee.attended,
#             name AS original_name,
#             e.name AS event_name AS webinar_topic,
#             eu.first_name, 
#             eu.last_name,
#             eu.email,
#             ee.duration AS time_in_session_minutes,
#             eu.profession,
#             eu.country,
#             eu.state AS county,
#             workplace AS work_place,
#             mfl_code,
#             facility_type AS type_of_facility,
#             facility_level AS level_of_facility,
#             title AS job_title
#             to_char(ee.enrollment_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS registration_time,
           
#             o.name AS organization_name, 
            
#         FROM 
#             public.event_enrolment_eventenrollment ee
#         JOIN 
#             public.event_enrolment_eventuser eu ON ee.user_id_id = eu.id
#         JOIN 
#             public.events_event e ON ee.event_id_id = e.id
#         JOIN 
#             public.organization_organization o ON e.organization_id_id = o.id
#         WHERE 
#             o.name='Population Services Kenya'
#     """,
#     bucket=BQ_BUCKET,
#     filename=JSON_FILENAME8,
#     export_format='csv',
#     postgres_conn_id=PG_CON_ID,
#     field_delimiter=',',
#     gzip=False,
#     task_concurrency=1,
#     execution_timeout=timedelta(minutes=10),
#     gcp_conn_id=BQ_CON_ID,
#     dag=dag,
# )


postgres_kma_report_to_gcs = PostgresToGCSOperator(
    task_id='postgres_report_kma_to_gcs',
    sql="""
        SELECT 
            e.title AS event_title,
            o.name AS organization_name,
            u.email,
            u.first_name,
            u.last_name,
            u.profession,
            u.country,
            u.board_number,
			ee.attended,
            to_char(ee.enrollment_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS registration_time,
            ee.duration
        FROM 
            public.event_enrolment_eventenrollment ee
        JOIN 
            public.events_event e ON ee.event_id_id = e.id
        JOIN 
            public.organization_organization o ON e.organization_id_id = o.id
        JOIN 
            public.event_enrolment_eventuser u ON ee.user_id_id = u.id
        WHERE 
            o.name = 'Kenya Medical Association'
    """,
    bucket=BQ_BUCKET,
    filename=JSON_FILENAME13,
    export_format='csv',
    postgres_conn_id=PG_CON_ID,
    field_delimiter=',',
    gzip=False,
    task_concurrency=1,
    execution_timeout=timedelta(minutes=10),
    gcp_conn_id=BQ_CON_ID,
    dag=dag,
)

# postgres_hospital_report_to_gcs = PostgresToGCSOperator(
#     task_id='postgres_hospital_report_to_gcs',
#      sql="""
#         SELECT
#             o.name AS organization_name,
#             '"' || z.event_name || '"' AS event_name,
#             z.name,
#             z.email,
#             z.profession,
#             z.country,
#             z.county,
#             z.duration,
#             z.board_number,
#             z.specialization,
#             z.gender,
#             z.location,
#             z.attended,
#             z.organization_category,
#             to_char(z.start_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS registration_timee
#         FROM 
#             (SELECT *
#             FROM public.stream_zoomreports
#             WHERE organization_category = 'hospital') z
#         JOIN 
#             public.organization_organization o ON z.organization_id_id = o.id
#     """,
#     bucket=BQ_BUCKET,
#     filename=JSON_FILENAME10,
#     export_format='csv',
#     postgres_conn_id=PG_CON_ID,
#     field_delimiter=',',
#     gzip=False,
#     task_concurrency=1,
#     execution_timeout=timedelta(minutes=10),
#     gcp_conn_id=BQ_CON_ID,
#     dag=dag,
# )

# postgres_pharma_report_to_gcs = PostgresToGCSOperator(
#     task_id='postgres_pharma_report_to_gcs',
#     sql="""
#         SELECT
#             o.name AS organization_name,
#             '"' || z.event_name || '"' AS event_name,
#             z.name,
#             z.email,
#             z.profession,
#             z.country,
#             z.county,
#             z.duration,
#             z.board_number,
#             z.specialization,
#             z.gender,
#             z.location,
#             z.attended,
#             z.organization_category,
#             to_char(z.start_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS registration_time
#         FROM 
#             (SELECT *
#             FROM public.stream_zoomreports
#             WHERE organization_category = 'pharma') z
#         JOIN 
#             public.organization_organization o ON z.organization_id_id = o.id
#     """,
#     bucket=BQ_BUCKET,
#     filename=JSON_FILENAME11,
#     export_format='csv',
#     postgres_conn_id=PG_CON_ID,
#     field_delimiter=',',
#     gzip=False,
#     task_concurrency=1,
#     execution_timeout=timedelta(minutes=10),
#     gcp_conn_id=BQ_CON_ID,
#     dag=dag,
# )

# postgres_association_report_to_gcs = PostgresToGCSOperator(
#     task_id='postgres_association_report_to_gcs',
#     sql="""
#         SELECT
#             o.name AS organization_name,
#             '"' || z.event_name || '"' AS event_name,
#             z.name,
#             z.email,
#             z.profession,
#             z.country,
#             z.county,
#             z.duration,
#             z.board_number,
#             z.specialization,
#             z.gender,
#             z.location,
#             z.attended,
#             z.organization_category,
#             to_char(z.start_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS registration_time
#         FROM 
#             (SELECT *
#             FROM public.stream_zoomreports
#             WHERE organization_category = 'association') z
#         JOIN 
#             public.organization_organization o ON z.organization_id_id = o.id
#     """,
#     bucket=BQ_BUCKET,
#     filename=JSON_FILENAME12,
#     export_format='csv',
#     postgres_conn_id=PG_CON_ID,
#     field_delimiter=',',
#     gzip=False,
#     task_concurrency=1,
#     execution_timeout=timedelta(minutes=10),
#     gcp_conn_id=BQ_CON_ID,
#     dag=dag,
# )

# send data to bigquery
# load_csv_pskenya_data_to_bq = GCSToBigQueryOperator(
#     task_id="load_csv_pskenya_data_to_bq",
#     bucket=BQ_BUCKET,
#     source_objects=[JSON_FILENAME8],
#     destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE8}",
#     create_disposition='CREATE_IF_NEEDED',
#     write_disposition="WRITE_TRUNCATE",
#     gcp_conn_id=BQ_CON_ID,
#     max_bad_records=100,
#     dag=dag,
# )

load_kma_csv_data_to_bq = GCSToBigQueryOperator(
    task_id="load_kma_csv_data_to_bq",
    bucket=BQ_BUCKET,
    source_objects=[JSON_FILENAME13],
    destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE13}",
    create_disposition='CREATE_IF_NEEDED',
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id=BQ_CON_ID,
    max_bad_records=100,
    dag=dag,
)

# load_csv_hospital_data_to_bq = GCSToBigQueryOperator(
#     task_id="load_csv_hospital_data_to_bq",
#     bucket=BQ_BUCKET,
#     source_objects=[JSON_FILENAME10],
#     destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE10}",
#     create_disposition='CREATE_IF_NEEDED',
#     write_disposition="WRITE_TRUNCATE",
#     gcp_conn_id=BQ_CON_ID,
#     max_bad_records=100,
#     dag=dag,
# )


# load_csv_pharma_data_to_bq = GCSToBigQueryOperator(
#     task_id="load_csv_pharma_data_to_bq",
#     bucket=BQ_BUCKET,
#     source_objects=[JSON_FILENAME11],
#     destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE11}",
#     create_disposition='CREATE_IF_NEEDED',
#     write_disposition="WRITE_TRUNCATE",
#     gcp_conn_id=BQ_CON_ID,
#     max_bad_records=100,
#     dag=dag,
# )

# load_csv_association_data_to_bq = GCSToBigQueryOperator(
#     task_id="load_csv_association_data_to_bq",
#     bucket=BQ_BUCKET,
#     source_objects=[JSON_FILENAME12],
#     destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE12}",
#     create_disposition='CREATE_IF_NEEDED',
#     write_disposition="WRITE_TRUNCATE",
#     gcp_conn_id=BQ_CON_ID,
#     max_bad_records=100,
#     dag=dag,
# )




# Transform >> to perform filtering, aggregation and joining

postgres_kma_report_to_gcs >> load_kma_csv_data_to_bq

# postgres_pskenya_report_to_gcs >> load_csv_pskenya_data_to_bq

# postgres_pharma_report_to_gcs >> load_csv_pharma_data_to_bq

# postgres_association_report_to_gcs >> load_csv_association_data_to_bq


