#!/bin/bash
set -e
bash build-docker.sh


docker run \
-it \
--rm \
--name=openagents-search \
openagents-search