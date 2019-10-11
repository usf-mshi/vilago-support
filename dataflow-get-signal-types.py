from __future__ import absolute_import

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.io import fileio

from datetime import datetime
import pytz

# options = PipelineOptions()
# google_cloud_options = options.view_as(GoogleCloudOptions)
# google_cloud_options.project = 'vilago-demo'
# google_cloud_options.job_name = 'get-signal-types'
# google_cloud_options.staging_location = 'staging/'
# google_cloud_options.temp_location = 'temp'
# options.view_as(StandardOptions).runner = 'DirectRunner'
# outputdir = 'directrunner-output/outputs'
# inputfiles = 'gs://tuh-eeg-corpus/seizure-v1.5.0/edf/train/01_tcp_ar/002/00000254/*/*.edf'

timestamp = datetime.now(pytz.timezone('US/Pacific')).__str__().replace(":", "").replace(" ", "-").replace(".", "")

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'vilago-demo'
google_cloud_options.job_name = 'get-signal-types-%s' % timestamp
google_cloud_options.staging_location = 'gs://vilago-dataflow-output/staging/'
google_cloud_options.temp_location = 'gs://vilago-dataflow-output/temp/'
options.view_as(StandardOptions).runner = 'DataflowRunner'
outputdir = 'gs://vilago-dataflow-output/output/%s' % timestamp
inputfiles = 'gs://tuh-eeg-corpus/seizure-v1.5.0/edf/train/*/*/*/*/*.edf'
#inputfiles = 'gs://tuh-eeg-corpus/seizure-v1.5.0/edf/train/01_tcp_ar/002/*/*/*.edf'


print('Starting beam job...')

p = beam.Pipeline(options=options)

files = p | 'Match EDF files' >> fileio.MatchFiles(inputfiles)

class GetSignalTypes(beam.DoFn):
    def process(self, path):
        import pyedflib
        from google.cloud import storage
        from tempfile import NamedTemporaryFile
        import os
        from pathlib import Path

        assert(path[0:5] == "gs://"), 'Expecting GCS URI (gs://) but got: %s' % path

        #client = storage.Client.from_service_account_json(Path(__file__).parent / 'vilago-demo-13b71d6e0147.json')

        client = storage.Client()

        f = NamedTemporaryFile(suffix=".edf", delete=False)
        print('Temp file created: %s' % f.name)
        client.download_blob_to_file(path, f)
        f.close()

        edf = pyedflib.EdfReader(f.name)
        labels = edf.getSignalLabels()

        os.unlink(f.name)

        return labels

class PairWithOne(beam.DoFn):
    def process(self, path):
        return [(path, 1)]

files \
    | 'Extract EDF path' >> beam.Map(lambda x: x.path) \
    | 'Prevent Fusion -- Dummy' >> beam.ParDo(PairWithOne()) \
    | 'Prevent Fusion -- Group' >> beam.GroupByKey() \
    | 'Prevent Fusion -- Split' >> beam.Keys() \
    | 'Parse EDF' >> beam.ParDo(GetSignalTypes()) \
    | 'Pair with 1' >> beam.ParDo(PairWithOne())  \
    | 'Group' >> beam.GroupByKey() \
    | 'Sum' >> beam.Map(lambda t: (t[0], sum(t[1]))) \
    | 'Format' >> beam.Map(lambda t: '%s, %d' % t) \
    | 'Output to folder' >> beam.io.WriteToText(outputdir, file_name_suffix='.txt')

p.run()

print('Beam job finished')