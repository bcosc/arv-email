#!/usr/bin/env python

import arvados
import json
import datetime
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import mimetypes
import base64
from apiclient import errors
import argparse
import datetime
import feed.date.rfc3339
from pytz import timezone
import sys
import os

def CreateMessage(sender, to, subject, message_text, file=''):
  message = MIMEMultipart()
  message['to'] = to
  message['from'] = sender
  message['subject'] = subject

  msg = MIMEText(message_text)
  message.attach(msg)

  if not file:
    return {'raw': base64.urlsafe_b64encode(message.as_string())}

  content_type, encoding = mimetypes.guess_type(file)

  if content_type is None or encoding is not None:
    content_type = 'application/octet-stream'
  main_type, sub_type = content_type.split('/', 1)
  if main_type == 'text':
    fp = open(file, 'rb')
    msg = MIMEText(fp.read(), _subtype=sub_type)
    fp.close()
  elif main_type == 'image':
    fp = open(file, 'rb')
    msg = MIMEImage(fp.read(), _subtype=sub_type)
    fp.close()
  elif main_type == 'audio':
    fp = open(file, 'rb')
    msg = MIMEAudio(fp.read(), _subtype=sub_type)
    fp.close()
  else:
    fp = open(file, 'rb')
    msg = MIMEBase(main_type, sub_type)
    msg.set_payload(fp.read())
    fp.close()
  filename = os.path.basename(file)
  msg.add_header('Content-Disposition', 'attachment', filename=filename)
  message.attach(msg)
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
        '-f', '--from', dest='from_email', required=False, default="arvtutorial1092015@gmail.com", help="The sender email address (required)")
    parser.add_argument(
        '-t', '--to', dest='to_email', required=False, default="arvtutorial1092015@gmail.com", help="The destination email address (required)")
    parser.add_argument(
        '-c', '--client', dest='client_secret', required=False, default="/home/bcosc/arv-email/client_secret.json", help="The path to your client_secret.json (required)")
    parser.add_argument(
        '-s', '--storage', dest='storage', required=False, default="/home/bcosc/arv-email/storage-temp.json", help="The path to your stored credentials (required)")
    parser.add_argument(
      	'-l', '--location', dest='location', required=False, default="qr1hi", help="The location of the cluster (required)")
    parser.add_argument(
        '-m', '--message', dest='message', required=False, default="no message!", help="The message body")
    parser.add_argument(
        '-a', '--attachment', dest='attachment', required=False, help="An attachment to the email")
    options = parser.parse_args()
    CLIENT_SECRET = options.client_secret

    store = file.Storage(options.storage)
    credz = store.get()
    if not credz or credz.invalid:
        flags = tools.argparser.parse_args(args=[])
        flow = client.flow_from_clientsecrets(CLIENT_SECRET, SCOPES)
        credz = tools.run_flow(flow, store, flags)
    GMAIL = build('gmail', 'v1', http=credz.authorize(Http()))
    if not options.attachment:
      message = CreateMessage(options.from_email, options.to_email, "header", options.message)
    else:
      message = CreateMessage(options.from_email, options.to_email, "header", options.message, options.attachment)
    SendMessage(GMAIL, 'me', message)

if __name__ == '__main__':
    main()
