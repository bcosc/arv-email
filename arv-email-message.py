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
    options = parser.parse_args()
    CLIENT_SECRET = options.client_secret

    # All current RunningOnServer
    message = 'Current running pipelines on wx7k5: \n\n'
    num_running = arvados.api('v1').pipeline_instances().list(
                      filters=[["state","=","RunningOnServer"]]).execute()["items_available"]
    message += 'There are currently ' + str(num_running) + ' pipelines running. \n\n'

    for instance_num in range(0,num_running):
        instance = arvados.api('v1').pipeline_instances().list(
                       filters=[["state","=","RunningOnServer"]]).execute()["items"][instance_num]
        for component, value in instance["components"].iteritems():
            if "job" in value:
                if value["job"]["state"] == 'Running':
                    message += instance["uuid"] + ' ' + component + ' started at: ' + value["job"]["started_at"] + '\n'
        message += '\n'

    store = file.Storage(options.storage)
    credz = store.get()
    if not credz or credz.invalid:
        flags = tools.argparser.parse_args(args=[])
        flow = client.flow_from_clientsecrets(CLIENT_SECRET, SCOPES)
        credz = tools.run_flow(flow, store, flags)
    GMAIL = build('gmail', 'v1', http=credz.authorize(Http()))

    message = CreateMessage(options.from_email, options.to_email, 'Pipeline digest', message)
    SendMessage(GMAIL, 'me', message)

if __name__ == '__main__':
    main()
