#!/usr/bin/env sh

docker build -f Dockerfile-worker . --tag cion/worker:latest
docker build -f Dockerfile-orchestrator . --tag cion/orchestrator:latest