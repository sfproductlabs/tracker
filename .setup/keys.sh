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


OUTDIR=$PWD/keys
CA_CERT=$OUTDIR/rootCa.crt
CA_KEY=$OUTDIR/rootCa.key
CA_SRL=$OUTDIR/rootCa.srl
CA_SUBJECT=/CN=rootCa/OU=PRODUCTION/O=SFPL/C=US/

#ca
generateCa

#nginx
generateModule nginx \
  "CN=NGINX, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:api.sfpl.io,dns:bps.sfpl.io,dns:sso.sfpl.io,dns:tr.sfpl.io

#cassandra
generateModule cassandra-server \
  "CN=CASSANDRA-SERVER, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:cassandra1.sfpl.io,dns:cassandra2.sfpl.io,dns:cassandra3.sfpl.io

generateModule cassandra-client \
  "CN=CASSANDRA-CLIENT, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:cassandra-client.sfpl.io

generateTrust cassandra-truststore cassandra-client

#nats
generateModule nats-server \
  "CN=NATS-SERVER, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:nats1.sfpl.io,dns:nats2.sfpl.io,dns:nats3.sfpl.io

generateModule nats-client \
  "CN=NATS-CLIENT, OU=PRODUCTION, O=SFPL, C=US" \
  san=dns:nats-client.sfpl.io

openssl genpkey -algorithm rsa -out $OUTDIR/priv.key
openssl rsa -in $OUTDIR/priv.key -pubout -out $OUTDIR/pub.key