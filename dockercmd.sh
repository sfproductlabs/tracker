#!/bin/sh
# executed on docker-image (aws task) startup

# AWS_REGION=eu-central-1

######
# read sensitive information from AWS EC2 parameter store
# this uses the 'ecsInstance' role which needs access rights on SSM::Get-Parameters
# (1) retrieve data from aws ssm store
# (2) extract value from json using jquery (-r is 'raw', no parentheses and proper new lines)

# TRACKER
# aws ssm get-parameters --names $SSM_ID_TRACKER_CERT --no-with-decryption --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > /app/tracker/server.crt
# aws ssm get-parameters --names $SSM_ID_TRACKER_KEY --with-decryption --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > /app/tracker/server.key

######
# start supervisor
supervisord -c /etc/supervisor/supervisord.conf
