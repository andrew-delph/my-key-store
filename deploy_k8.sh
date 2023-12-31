#!/bin/bash

# Exit on any error
set -e

# eval $(minikube docker-env)

STATEFULSET_NAME="store"
NAMESPACE="default"

TAG=$(date +%s)
NEW_IMAGE=ghcr.io/andrew-delph/main:$TAG

bazel run --execution_log_json_file=events.json //main:image_push -- -dst=$NEW_IMAGE


ROLL_OUT_FLAG=0
# Use getopts to check for the -r (rollout) flag
while getopts ":r" opt; do
  case $opt in
    r)
      ROLL_OUT_FLAG=1
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done


SECONDS=0  # Reset the SECONDS variable


if [ $ROLL_OUT_FLAG -eq 0 ]; then
    echo "The 'rollout' flag is not set. Setting up."
    (cd operator && kustomize build config/crd | kubectl apply -f - || true)
    kubectl apply -f ./operator/config/samples/ || true
else
    echo "The 'rollout' flag is set."
    (cd operator && make install || true)
    kubectl create -f ./operator/config/samples/ || true
    kubectl patch mykeystore store --type=merge -p "{\"spec\":{\"image\":\"$NEW_IMAGE\"}}" || true
fi






