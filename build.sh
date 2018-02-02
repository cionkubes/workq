#!/usr/bin/env sh

docker build -f Dockerfile-worker . --tag cion/worker:${1:-latest}
docker build -f Dockerfile-orchestrator . --tag cion/orchestrator:${1:-latest}
