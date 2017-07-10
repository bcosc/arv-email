#!/usr/bin/env python

import arvados
import json
import datetime
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from email.mime.text import MIMEText
import base64
from apiclient import errors
import argparse
import datetime
import feed.date.rfc3339
from pytz import timezone

def RFC3339Convert_to_readable(rfc_time):
    default_time_offset = "EST"
    feed.date.rfc3339.set_default_time_offset(default_time_offset)
    tf = feed.date.rfc3339.tf_from_timestamp(rfc_time)
    ts = feed.date.rfc3339.timestamp_from_tf(tf)
    dt = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S' + default_time_offset)
    fixed_hour = dt.hour
    noon = 'AM'
    if dt.hour > 12:
        fixed_hour = dt.hour-12
        noon = 'PM'
    return '%s-%s-%s %s:%s %s' % (dt.year, dt.month, dt.day, fixed_hour, dt.minute, noon)

def RFC3339Convert_to_dt(rfc_time):
    default_time_offset = "EST"
    feed.date.rfc3339.set_default_time_offset(default_time_offset)
    timefloat = feed.date.rfc3339.tf_from_timestamp(rfc_time)
    timestamp = feed.date.rfc3339.timestamp_from_tf(timefloat)
    dt = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S' + default_time_offset)
    return dt

def Time_diff(early, late):
    diff = late - early
    diff_chop_ms = diff - datetime.timedelta(microseconds=diff.microseconds)
    return diff_chop_ms

def Current_time():
    est = timezone('US/Eastern')
    time = datetime.datetime.now(est)
    time = time.replace(microsecond=0)
    time = time.replace(tzinfo=None)
    return time


#num_running = arvados.api('v1').pipeline_instances().list(
#                      filters=[["state","=","RunningOnServer"]]).execute()["items_available"]
#message = 'There are currently %s pipelines running on %s. \n\n' % (str(num_running), options.location)

#for instance_num in range(0,num_running):
message = ''
instance = arvados.api('v1').pipeline_instances().list(
                  filters=[["state","=","RunningOnServer"]]).execute()["items"][0]#[instance_num]
print instance
#print instance["components"]["cwl-runner"]["job"]["components"].keys()
for component, value in instance["components"]["cwl-runner"]["job"]["components"].iteritems():
  #print component
  job = arvados.api('v1').jobs().list(filters=[["uuid","=",value]]).execute()
  #print job["items"][0].keys()
  #print job["items"][0]["running"]
  if job["items"][0]["running"]:
    message += '%s\n%s started at: %s\n' % (instance["uuid"], component, RFC3339Convert_to_readable(job["items"][0]["created_at"]))
    message += '%s has been running for %s\n' %(component, Time_diff(RFC3339Convert_to_dt(job["items"][0]["created_at"]),Current_time()))
  if not job["items"][0]["running"] and (job["items"][0]["success"] is None):
    message += '%s\n%s is queued, it was created at: %s\n' % (instance["uuid"], component, RFC3339Convert_to_readable(job["items"][0]["created_at"]))
    message += '\n'
print message
