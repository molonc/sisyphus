#!/bin/bash

# Must be logged in to ACR via "az acr login --name REGISTRY_NAME"

MODULE=$1
VERSION=$2
REGISTRY=$3

# pull docker image
docker pull "quay.io/singlecellpipeline/single_cell_pipeline_${MODULE}:${VERSION}"

# tag it
docker tag "quay.io/singlecellpipeline/single_cell_pipeline_${MODULE}:${VERSION}" "${REGISTRY}/singlecellpipeline/single_cell_pipeline_${MODULE}:${VERSION}"

# push to registry
docker push "${REGISTRY}/singlecellpipeline/single_cell_pipeline_${MODULE}:${VERSION}"