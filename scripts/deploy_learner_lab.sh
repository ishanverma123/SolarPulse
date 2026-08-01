#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 7 ]]; then
  echo "Usage: $0 <stack-name> <key-name> <vpc-id> <public-subnet-id> <allowed-ssh-cidr> <lambda-role-arn> <instance-profile-name> [region]"
  exit 1
fi

STACK_NAME="$1"
KEY_NAME="$2"
VPC_ID="$3"
SUBNET_ID="$4"
ALLOWED_SSH_CIDR="$5"
LAMBDA_ROLE_ARN="$6"
INSTANCE_PROFILE_NAME="$7"
REGION="${8:-eu-west-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

aws cloudformation deploy \
  --region "${REGION}" \
  --stack-name "${STACK_NAME}" \
  --template-file "${PROJECT_ROOT}/infra/learner_lab_stack.yaml" \
  --parameter-overrides \
    ProjectName=solarpulse \
    KeyName="${KEY_NAME}" \
    VpcId="${VPC_ID}" \
    PublicSubnetId="${SUBNET_ID}" \
    AllowedSshCidr="${ALLOWED_SSH_CIDR}" \
    ExistingLambdaRoleArn="${LAMBDA_ROLE_ARN}" \
    ExistingInstanceProfileName="${INSTANCE_PROFILE_NAME}"

aws cloudformation describe-stacks \
  --region "${REGION}" \
  --stack-name "${STACK_NAME}" \
  --query 'Stacks[0].Outputs'
echo "Stack deployed. Next: sync this project to the Spark host and run producer/batch/speed jobs."
