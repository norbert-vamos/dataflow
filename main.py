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

"""
A front end to initate a dataflow pipeline
"""

import datetime
import logging
import os
from google.appengine.ext import ndb
import webapp2

from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials


class LaunchJob(webapp2.RequestHandler):
  """Launch the Dataflow pipeline using a job template."""

  def get(self):

    # Comment out the following check to allow non-cron-initiated requests.
    is_cron = self.request.headers.get('X-Appengine-Cron', False)
    if not is_cron:
      return 'Blocked.'

    # These env vars are set in app.yaml.
    PROJECT = os.environ['PROJECT']
    BUCKET = os.environ['BUCKET']
    TEMPLATE = os.environ['TEMPLATE_NAME']

    # Because we're using the same job name each time, if you try to launch one
    # job while another is still running, the second will fail.
    JOBNAME = PROJECT + '-query-template'

    credentials = GoogleCredentials.get_application_default()

    service = build('dataflow', 'v1b3', credentials=credentials)

    # Setup the dataflow configs
    BODY = {
        "jobName": "{jobname}".format(jobname=JOBNAME),
        "gcsPath": "gs://{bucket}/templates/{template}".format(
            bucket=BUCKET, template=TEMPLATE),
        "environment": {
            "tempLocation": "gs://{bucket}/temp".format(bucket=BUCKET),
            "zone": "us-central1-f"
        }
    }

    dfrequest = service.projects().templates().create(
        projectId=PROJECT, body=BODY)

    dfresponse = dfrequest.execute()

    logging.info(dfresponse)

    self.response.write('Done')


class MainPage(webapp2.RequestHandler):
  def get(self):
    self.response.write('nothing to see.')


# Handle the proper URL string to kick off the dataflow job
app = webapp2.WSGIApplication(
    # [('/', MainPage),
    #('/anotherJob', anotherJob),
    #('/yetAnotherJob', yetAnotherJob),
    [('/launchtemplatejob', LaunchJob)],
    debug=True)
