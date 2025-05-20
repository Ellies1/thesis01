#!/bin/bash

# Necessary because GCS client library has trouble with symlinks
cat /opt/contistuff/keyfile/gcs_keyfile.json > /opt/contistuff/gcs_keyfile.json
chmod 777 /opt/contistuff/gcs_keyfile.json

/opt/entrypoint.sh "$@"