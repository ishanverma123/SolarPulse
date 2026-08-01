#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 5 ]]; then
  echo "Usage: $0 <stack-name> <key-name> <vpc-id> <public-subnet-id> <allowed-ssh-cidr> [region]"
  exit 1
fi

STACK_NAME="$1"
KEY_NAME="$2"
VPC_ID="$3"
SUBNET_ID="$4"
ALLOWED_SSH_CIDR="$5"
REGION="${6:-eu-west-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

aws cloudformation deploy \
  --region "${REGION}" \
  --stack-name "${STACK_NAME}" \
  --template-file "${PROJECT_ROOT}/infra/learner_lab_stack.yaml" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ProjectName=solarpulse \
    KeyName="${KEY_NAME}" \
    VpcId="${VPC_ID}" \
    PublicSubnetId="${SUBNET_ID}" \
    AllowedSshCidr="${ALLOWED_SSH_CIDR}"

aws cloudformation describe-stacks \
  --region "${REGION}" \
  --stack-name "${STACK_NAME}" \
  --query 'Stacks[0].Outputs'
echo "Stack deployed. Next: sync this project to the Spark host and run producer/batch/speed jobs."
