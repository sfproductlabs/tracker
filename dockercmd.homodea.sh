#!/bin/sh
# executed on docker-image (aws task) startup

AWS_REGION=eu-central-1
CONFIG_FILE=./config.homodea.json

######

# read sensitive information from AWS EC2 parameter store
# this uses the 'ecsInstance' role which needs access rights on SSM::Get-Parameters
# (1) retrieve data from aws ssm store
# (2) extract value from json using jquery (-r is 'raw', no parentheses and proper new lines)

mkdir pem

# TRACKER
aws ssm get-parameters --names $SSM_ID_TRACKER_CACERT --no-with-decryption --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/tracker.cacert.pem
aws ssm get-parameters --names $SSM_ID_TRACKER_CERT   --no-with-decryption --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/tracker.cert.pem
aws ssm get-parameters --names $SSM_ID_TRACKER_KEY    --with-decryption    --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/tracker.key.pem

# CASSANDRA
aws ssm get-parameters --names $SSM_ID_CASSANDRA_CACERT --no-with-decryption --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/cassandra.cacert.pem
aws ssm get-parameters --names $SSM_ID_CASSANDRA_CERT   --no-with-decryption --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/cassandra.cert.pem
aws ssm get-parameters --names $SSM_ID_CASSANDRA_KEY    --with-decryption    --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/cassandra.key.pem

# NATS
aws ssm get-parameters --names $SSM_ID_NATS_CACERT --no-with-decryption --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/nats.cacert.pem
aws ssm get-parameters --names $SSM_ID_NATS_CERT   --no-with-decryption --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/nats.cert.pem
aws ssm get-parameters --names $SSM_ID_NATS_KEY    --with-decryption    --region $AWS_REGION --output json | jq -r '.Parameters[0] | .Value' > ./pem/nats.key.pem

######
# modify config.json according to environment args

# this is a work-around due to jq not supporting 
# in-place editing the same file
TMP_FILE=$(mktemp)

jq -r --argjson TRACKER_DOMAINS $TRACKER_DOMAINS '.Domains |= $TRACKER_DOMAINS' $CONFIG_FILE > $TMP_FILE
mv -f $TMP_FILE $CONFIG_FILE

jq -r --argjson TRACKER_CASSANDRA_HOSTS $TRACKER_CASSANDRA_HOSTS '(.Notify[]  | select(.Service == "cassandra") | .Hosts) |= $TRACKER_CASSANDRA_HOSTS' $CONFIG_FILE > $TMP_FILE
mv -f $TMP_FILE $CONFIG_FILE

jq -r --argjson TRACKER_NATS_HOSTS $TRACKER_NATS_HOSTS '(.Notify[]  | select(.Service == "nats") | .Hosts) |= $TRACKER_NATS_HOSTS' $CONFIG_FILE > $TMP_FILE
mv -f $TMP_FILE $CONFIG_FILE

jq -r --argjson TRACKER_NATS_HOSTS $TRACKER_NATS_HOSTS '(.Consume[] | select(.Service == "nats") | .Hosts) |= $TRACKER_NATS_HOSTS' $CONFIG_FILE > $TMP_FILE
mv -f $TMP_FILE $CONFIG_FILE

#TODO: modify the new 'UrlPrefixFilter'

######
# start supervisor
supervisord -c /etc/supervisor/supervisord.conf
