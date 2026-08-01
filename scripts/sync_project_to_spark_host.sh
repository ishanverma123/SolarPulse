#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <ec2-user@host> <path-to-private-key>"
  exit 1
fi

TARGET_HOST="$1"
PRIVATE_KEY="$2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_USER="${TARGET_HOST%%@*}"

ssh -i "${PRIVATE_KEY}" -o StrictHostKeyChecking=no "${TARGET_HOST}" \
  "sudo mkdir -p /opt/solarpulse && sudo chown -R ${TARGET_USER}:${TARGET_USER} /opt/solarpulse"

rsync -avz \
  --rsync-path="sudo rsync" \
  --exclude '.venv' \
  --exclude '__pycache__' \
  --exclude 'output' \
  -e "ssh -i ${PRIVATE_KEY} -o StrictHostKeyChecking=no" \
  "${PROJECT_ROOT}/" "${TARGET_HOST}:/opt/solarpulse/"

echo "Project synced to ${TARGET_HOST}:/opt/solarpulse/"
