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

def main():

    SCOPES = [
      'https://mail.google.com/',
      'https://www.googleapis.com/auth/gmail.send',
      'https://www.googleapis.com/auth/gmail.compose'
    ]
    arv = arvados.api('v1')

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--from', dest='from_email', required=True, help="The sender email address (required)")
    parser.add_argument(
        '-t', '--to', dest='to_email', required=True, help="The destination email address (required)")
    parser.add_argument(
        '-c', '--client', dest='client_secret', required=True, help="The path to your client_secret.json (required)")
    parser.add_argument(
        '-s', '--storage', dest='storage', required=True, help="The path to your stored credentials (required)")
    parser.add_argument(
	'-l', '--location', dest='location', required=True, help="The location of the cluster (required)")
    options = parser.parse_args()
    CLIENT_SECRET = options.client_secret

    # All current RunningOnServer
    num_running = arvados.api('v1').pipeline_instances().list(
                      filters=[["state","=","RunningOnServer"]]).execute()["items_available"]
    message = 'There are currently %s pipelines running on %s. \n\n' % (str(num_running), options.location)

    for instance_num in range(0,num_running):
        instance = arvados.api('v1').pipeline_instances().list(
                       filters=[["state","=","RunningOnServer"]]).execute()["items"][instance_num]
        for component, value in instance["components"].iteritems():
            if "job" in value:
                if value["job"]["state"] == 'Running':
		    message += '%s\n%s started at: %s\n' % (instance["uuid"], component, RFC3339Convert_to_readable(value["job"]["started_at"]))
		    message += '%s has been running for %s\n' %(component, Time_diff(RFC3339Convert_to_dt(value["job"]["started_at"]),Current_time()))
		if value["job"]["state"] == 'Queued':
		    message += '%s\n%s is queued, it was created at: %s\n' % (instance["uuid"], component, RFC3339Convert_to_readable(value["job"]["created_at"]))
        message += '\n'

    store = file.Storage(options.storage)
    credz = store.get()
    if not credz or credz.invalid:
        flags = tools.argparser.parse_args(args=[])
        flow = client.flow_from_clientsecrets(CLIENT_SECRET, SCOPES)
        credz = tools.run_flow(flow, store, flags)
    GMAIL = build('gmail', 'v1', http=credz.authorize(Http()))

    message = CreateMessage(options.from_email, options.to_email, '%s pipelines running on %s' % (str(num_running), options.location), message)
    SendMessage(GMAIL, 'me', message)

if __name__ == '__main__':
    main()
