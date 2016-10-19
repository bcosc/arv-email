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

def CreateMessage(sender, to, subject, message_text):
  message = MIMEText(message_text)
  message['to'] = to
  message['from'] = sender
  message['subject'] = subject
  return {'raw': base64.urlsafe_b64encode(message.as_string())}

def SendMessage(service, user_id, message):
  try:
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print 'Message Id: %s' % message['id']
    return message
  except errors.HttpError, error:
    print 'An error occurred: %s' % error

def RFC3339Convert(rfc_time):
    default_time_offset = "EST"
    feed.date.rfc3339.set_default_time_offset(default_time_offset)
    timefloat = feed.date.rfc3339.tf_from_timestamp(rfc_time)
    timestamp = feed.date.rfc3339.timestamp_from_tf(timefloat)
    dt = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S' + default_time_offset)
    fixed_hour = dt.hour
    noon = 'AM'
    if dt.hour > 12:
        fixed_hour = dt.hour-12
        noon = 'PM'
    return '%s-%s %s:%s %s' % (dt.month, dt.day, fixed_hour, dt.minute, noon)

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

foo = RFC3339Convert_to_dt('2016-02-02T19:36:24.060314000Z')
print foo

def Current_time():
    est = timezone('US/Eastern')
    time = datetime.datetime.now(est)
    time = time.replace(microsecond=0)
    time = time.replace(tzinfo=None)
    return time

print Time_diff(RFC3339Convert_to_dt('2016-02-02T19:36:24.060314000Z'),Current_time())
