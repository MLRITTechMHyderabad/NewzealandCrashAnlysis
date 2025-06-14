import json
import csv
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

class ExtractGCSPathDoFn(beam.DoFn):
    def process(self, element):
        try:
            decoded = element.decode('utf-8').strip()
            message = json.loads(decoded)
            gcs_path = message.get('gcs_path')
            if gcs_path:
                yield gcs_path
            else:
                logging.warning(f"No 'gcs_path' found in message: {message}")
        except Exception as e:
            logging.error(f"Failed to decode PubSub message: {e}")

class ParseCSVLineDoFn(beam.DoFn):
    def __init__(self): 
        self.header_skipped = False

    def process(self, line):
        if not self.header_skipped:
            self.header_skipped = True
            return
        reader = csv.reader([line])
        for row in reader:
            try:
                if len(row) != 42:
                    logging.error(f"Invalid row length: {len(row)}. Row: {row}")
                    return
                yield {
                    'bicycle': int(row[0]),
                    'bridge': int(row[1]),
                    'bus': int(row[2]),
                    'carstationwagon': int(row[3]),
                    'crashdirectiondescription': row[4],
                    'crashlocation1': row[5],
                    'crashlocation2': row[6],
                    'crashseverity': row[7],
                    'crashshdescription': row[8],
                    'crashyear': int(row[9]),
                    'debris': int(row[10]),
                    'directionroledescription': row[11],
                    'fatalcount': int(row[12]),
                    'flathill': row[13],
                    'light': row[14],
                    'minorinjurycount': int(row[15]),
                    'moped': int(row[16]),
                    'motorcycle': int(row[17]),
                    'numberoflanes': int(row[18]),
                    'objectthrownordropped': int(row[19]),
                    'othervehicletype': int(row[20]),
                    'parkedvehicle': int(row[21]),
                    'region': row[22],
                    'roadcharacter': row[23],
                    'roadlane': row[24],
                    'roadsurface': row[25],
                    'roadworks': int(row[26]),
                    'schoolbus': int(row[27]),
                    'seriousinjurycount': int(row[28]),
                    'speedlimit': int(row[29]),
                    'strayanimal': int(row[30]),
                    'streetlight': row[31],
                    'trafficcontrol': row[32],
                    'unknownvehicletype': int(row[33]),
                    'waterriver': int(row[34]),
                    'weathera': row[35],
                    'crashfinancialstartyear': int(row[36]),
                    'directionindex': float(row[37]),
                    'urbanindex': float(row[38]),
                    'streetlightindex': float(row[39]),
                    'speedcategory': row[40],
                    'anyinjury': int(row[41])
                }
            except Exception as e:
                logging.error(f"Error parsing row: {row} | Error: {e}")

def run():
    input_subscription = 'projects/nz-traffic-crash-analysis/subscriptions/csv-file-sub'
    output_table = 'crash_analysis_dataset.trafficdata'

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read PubSub Messages' >> beam.io.ReadFromPubSub(subscription=input_subscription)
            | 'Extract GCS File Path' >> beam.ParDo(ExtractGCSPathDoFn())
            | 'Read File Contents' >> beam.io.ReadAllFromText()
            | 'Parse CSV Rows' >> beam.ParDo(ParseCSVLineDoFn())
            # | 'beam.Map(print)'>> beam.Map(lambda x: logging.info(f"Parsed row: {x}"))
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                output_table,
                schema={
                    'fields': [
                        {'name': 'bicycle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'bridge', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'bus', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'carstationwagon', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'crashdirectiondescription', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'crashlocation1', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'crashlocation2', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'crashseverity', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'crashshdescription', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'crashyear', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'debris', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'directionroledescription', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'fatalcount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'flathill', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'light', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'minorinjurycount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'moped', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'motorcycle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'numberoflanes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'objectthrownordropped', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'othervehicletype', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'parkedvehicle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'roadcharacter', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'roadlane', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'roadsurface', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'roadworks', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'schoolbus', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'seriousinjurycount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'speedlimit', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'strayanimal', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'streetlight', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'trafficcontrol', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'unknownvehicletype', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'waterriver', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'weathera', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'crashfinancialstartyear', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'directionindex', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'urbanindex', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'streetlightindex', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'speedcategory', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'anyinjury', 'type': 'INTEGER', 'mode': 'NULLABLE'}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    logging.info("Pipeline completed successfully.")
