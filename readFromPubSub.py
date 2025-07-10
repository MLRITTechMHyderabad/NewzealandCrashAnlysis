import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import io
import logging

# Expected schema with type mappings
EXPECTED_SCHEMA = {
    "bicycle": int,
    "bridge": int,
    "bus": int,
    "carstationwagon": int,
    "crashdirectiondescription": str,
    "crashlocation1": str,
    "crashlocation2": str,
    "crashseverity": str,
    "crashshdescription": str,
    "crashyear": int,
    "debris": int,
    "directionroledescription": str,
    "fatalcount": int,
    "flathill": str,
    "light": str,
    "minorinjurycount": int,
    "moped": int,
    "motorcycle": int,
    "numberoflanes": int,
    "objectthrownordropped": int,
    "othervehicletype": int,
    "parkedvehicle": int,
    "region": str,
    "roadcharacter": str,
    "roadlane": str,
    "roadsurface": str,
    "roadworks": int,
    "schoolbus": int,
    "seriousinjurycount": int,
    "speedlimit": int,
    "strayanimal": int,
    "streetlight": str,
    "trafficcontrol": str,
    "unknownvehicletype": int,
    "waterriver": int,
    "weathera": str,
    "crashfinancialstartyear": int,
    "directionindex": float,
    "urbanindex": float,
    "streetlightindex": float,
    "speedcategory": str,
    "anyinjury": int
}

# CSV Parser & Cleaner
class ParseAndCleanCSV(beam.DoFn):
    def process(self, element):
        try:
            line = element.decode('utf-8')
            reader = csv.DictReader(io.StringIO(line), fieldnames=list(EXPECTED_SCHEMA.keys()))
            for row in reader:
                # Skip if this is a header row
                if row[list(EXPECTED_SCHEMA.keys())[0]].lower() == list(EXPECTED_SCHEMA.keys())[0].lower():
                    continue

                cleaned_row = {}
                for key, cast_type in EXPECTED_SCHEMA.items():
                    value = row.get(key, '').strip()
                    try:
                        cleaned_row[key] = cast_type(value) if value else None
                    except Exception as e:
                        logging.warning(f"Failed casting key={key} value={value} error={e}")
                        cleaned_row[key] = None
                yield cleaned_row
        except Exception as e:
            logging.error(f"Failed processing row: {e}")

def run():
    options = PipelineOptions(
        streaming=True,
        project='maximal-zoo-459404-h0',
        region='us-central1',
        job_name='crash-dataflow-pipeline3',
        temp_location='gs://newzealandpart2/temp',
        runner='DataflowRunner',
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                subscription='projects/maximal-zoo-459404-h0/subscriptions/newzealand-crash-subscription')
            | 'ParseAndCleanCSV' >> beam.ParDo(ParseAndCleanCSV())
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                table='maximal-zoo-459404-h0:crashdataautomation.Newzealandcrashdata_v2',
                schema={
                    "fields": [
                        {"name": k, "type": ("FLOAT" if v == float else "INTEGER" if v == int else "STRING"), "mode": "NULLABLE"}
                        for k, v in EXPECTED_SCHEMA.items()
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                custom_gcs_temp_location='gs://newzealandpart2/temp'
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
