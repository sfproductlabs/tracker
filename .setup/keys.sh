#!/bin/bash

# CA
CA_DAYSVALID=30000

# OTHER
STOREPASS=YInKGOL6P7kzJCx
KEYPASS=YInKGOL6P7kzJCx
TRUSTSTOREPASS=YInKGOL6P7kzJCx
VALIDITY=30000

#########################################################

function generateCa {
  # delete old ca
  rm -f $CA_KEY > /dev/null
  rm -f $CA_CERT > /dev/null
  rm -f $CA_SRL > /dev/null

  # create new ca
  openssl req -new -x509 -nodes \
    -subj $CA_SUBJECT \
    -keyout $CA_KEY \
    -out $CA_CERT \
    -days $CA_DAYSVALID
}

function generateModule {
  # remove old files
  rm -f $OUTDIR/$1.jks > /dev/null
  rm -f $OUTDIR/$1.p12 > /dev/null
  rm -f $OUTDIR/$1.crt > /dev/null
  rm -f $OUTDIR/$1.key > /dev/null
  rm -f $OUTDIR/$1.csr > /dev/null

  # 1) generate keypair
  keytool -genkeypair -keyalg RSA -alias $1 \
    -keystore $OUTDIR/$1.jks \
    -storepass $STOREPASS \
    -keypass $KEYPASS \
    -validity $VALIDITY \
    -keysize 2048 \
    -dname "$2"

  # 2) generate cert request
  keytool -certreq -alias $1 \
    -keystore $OUTDIR/$1.jks \
    -file $OUTDIR/$1.csr \
    -keypass $KEYPASS \
    -storepass $STOREPASS \
    -ext $3 \
    -dname "$2"

  # 3) sign cert using ca
  openssl x509 -req -CA $CA_CERT -CAkey $CA_KEY \
    -in $OUTDIR/$1.csr \
    -out $OUTDIR/$1.crt \
    -days $VALIDITY \
    -CAcreateserial \
    -CAserial $CA_SRL

  # 4) add rootCa to store
  keytool -importcert -alias rootCa \
    -keystore $OUTDIR/$1.jks \
    -file $CA_CERT \
    -noprompt \
    -keypass $KEYPASS \
    -storepass $STOREPASS

  # 5) import signed certificate
  keytool -importcert -alias $1 \
    -keystore $OUTDIR/$1.jks \
    -file $OUTDIR/$1.crt \
    -noprompt \
    -keypass $KEYPASS \
    -storepass $STOREPASS

  # 6) create pkcs12 variant of keystore
  keytool -importkeystore \
    -srcalias $1 \
    -srckeystore $OUTDIR/$1.jks \
    -destkeystore $OUTDIR/$1.p12 \
    -deststoretype PKCS12 \
    -keypass $KEYPASS \
    -storepass $STOREPASS \
    -srcstorepass $STOREPASS

  # 7) extract private-key from keystore
  openssl pkcs12 -nocerts \
    -in $OUTDIR/$1.p12 \
    -out $OUTDIR/$1.key \
    -passin pass:$STOREPASS \
    -passout pass:$STOREPASS

  # 8) save private-key unencrypted
  openssl rsa -in $OUTDIR/$1.key -out $OUTDIR/$1.key -passin pass:$KEYPASS
}

function generateTrust {
  # remove old files
  rm -f $OUTDIR/$1.jks > /dev/null

  # 1) add rootCa to truststore
  keytool -importcert -alias rootCa \
    -keystore $OUTDIR/$1.jks \
    -file $CA_CERT \
    -noprompt \
    -keypass $KEYPASS \
    -storepass $TRUSTSTOREPASS

  # 2) import signed client certificate
  keytool -importcert -alias $2 \
    -keystore $OUTDIR/$1.jks \
    -file $OUTDIR/$2.crt \
    -noprompt \
    -keypass $KEYPASS \
    -storepass $TRUSTSTOREPASS
}

#########################################################

######## staging #########
OUTDIR=$PWD/keys/staging
CA_CERT=$OUTDIR/rootCa.crt
CA_KEY=$OUTDIR/rootCa.key
CA_SRL=$OUTDIR/rootCa.srl
CA_SUBJECT=/CN=rootCa/OU=STAGING/O=SFPL/C=US/

#ca
generateCa

#nginx
generateModule nginx \
  "CN=NGINX, OU=STAGING, O=SFPL, C=US" \
  san=dns:api.staging.sfpl.com,dns:bps.staging.sfpl.com,dns:sso.staging.sfpl.com

#cassandra
generateModule cassandra-server \
  "CN=CASSANDRA-SERVER, OU=STAGING, O=SFPL, C=US" \
  san=dns:cassandra1.staging.sfpl.com,dns:cassandra2.staging.sfpl.com,dns:cassandra3.staging.sfpl.com

generateModule cassandra-client \
  "CN=CASSANDRA-CLIENT, OU=STAGING, O=SFPL, C=US" \
  san=dns:cassandra-client.staging.sfpl.com

generateTrust cassandra-truststore cassandra-client 

#nats
generateModule nats-server \
  "CN=NATS-SERVER, OU=STAGING, O=SFPL, C=US" \
  san=dns:nats1.staging.sfpl.com,dns:nats2.staging.sfpl.com,dns:nats3.staging.sfpl.com

generateModule nats-client \
  "CN=NATS-CLIENT, OU=STAGING, O=SFPL, C=US" \
  san=dns:nats-client.staging.sfpl.com

openssl genpkey -algorithm rsa -out $OUTDIR/priv.key
openssl rsa -in $OUTDIR/priv.key -pubout -out $OUTDIR/pub.key

######## production #########
OUTDIR=$PWD/keys/production
CA_CERT=$OUTDIR/rootCa.crt
CA_KEY=$OUTDIR/rootCa.key
CA_SRL=$OUTDIR/rootCa.srl
CA_SUBJECT=/CN=rootCa/OU=PRODUCTION/O=SFPL/C=US/

#ca
generateCa

#nginx
generateModule nginx \
  "CN=NGINX, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:api.sfpl.com,dns:bps.sfpl.com,dns:sso.sfpl.com

#cassandra
generateModule cassandra-server \
  "CN=CASSANDRA-SERVER, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:cassandra1.sfpl.com,dns:cassandra2.sfpl.com,dns:cassandra3.sfpl.com

generateModule cassandra-client \
  "CN=CASSANDRA-CLIENT, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:cassandra-client.sfpl.com

generateTrust cassandra-truststore cassandra-client

#nats
generateModule nats-server \
  "CN=NATS-SERVER, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:nats1.sfpl.com,dns:nats2.sfpl.com,dns:nats3.sfpl.com

generateModule nats-client \
  "CN=NATS-CLIENT, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:nats-client.sfpl.com

openssl genpkey -algorithm rsa -out $OUTDIR/priv.key
openssl rsa -in $OUTDIR/priv.key -pubout -out $OUTDIR/pub.key