# Copyright 2017 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""An ETL process to get data from a BigQuery query, apply transformations and save to a BigQuery table"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions


# Simple transform to change the key values of a data set
def simpleTransform(record):
    return {'Major': record['string_field_1'], 'Category': record['string_field_2']}

# Main Function
def run(project, pipeline_options):
    options = pipeline_options
    google_cloud_options = options.view_as(GoogleCloudOptions)

    # GCP project id
    google_cloud_options.project = project

    pipeline = beam.Pipeline(options=options)

    # Query to BigQuery
    bq_query = 'SELECT * FROM majors.data'

    (pipeline
     # Extract data from BigQuery using specified query
     | 'Extract from BQ' >> beam.io.Read(beam.io.BigQuerySource(query=bq_query))
     # Simple test transformation that shows how to change data
     | 'Simple Transform' >> beam.Map(simpleTransform)
     # Write data to an existing BigQuery table with schema specified inline
     # create_disposition 'CREATE_IF_NEEDED' - means that we are creating a table if it's not there
     # write_disposition 'WRITE_APPEND' - means we are appending data to table
     | 'Write to BQ' >> beam.io.WriteToBigQuery('output.data',
                                                schema='Major:STRING, Category:STRING',
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    pipeline.run().wait_until_finish()


if __name__ == '__main__':
    run()
