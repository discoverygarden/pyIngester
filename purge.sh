#!/bin/bash

FH=$FEDORA_HOME
USERNAME="fedoraAdmin"
PASSWORD="fedoraAdmin"
HOST="localhost"
PORT="8080"
PROT="http"
MSG="Nuked via purge.sh"

#if [ -z "$FH" ]; then
#  FH="/usr/local/fedora"
#fi

#PURGE=$FH/client/bin/fedora-purge.sh

#if [ -x $PURGE ]; then
  for PID in $@; do
    curl -XDELETE -u$USERNAME:$PASSWORD "$PROT://$HOST:$PORT/fedora/objects/$PID"
    #$PURGE ${HOST}:${PORT} ${USERNAME} ${PASSWORD} ${PID} ${PROT} "${MSG}" 
  done
#else
#  echo $PURGE is not executable!
#fi
