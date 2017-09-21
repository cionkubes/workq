#!/usr/bin/env sh

docker build -f Dockerfile-worker . --tag tiptk/tipimages:cion-worker_latest
docker build -f Dockerfile-orchestrator . --tag tiptk/tipimages:cion-orchestrator_latest