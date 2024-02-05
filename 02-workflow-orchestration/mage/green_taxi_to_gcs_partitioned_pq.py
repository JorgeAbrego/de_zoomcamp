import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/keys/vld-creds.json"

bucket_name = 'dtc_data_lake_vld-dtc-de-00001'
project_id = 'vld-dtc-de-00001'

table_name = 'green_taxi_data'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    print(data.shape)
    table = pa.Table.from_pandas(data)
    print("Table Created")
    gcs = pa.fs.GcsFileSystem()
    print("GCS Filesystem created")
    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem=gcs
    )


