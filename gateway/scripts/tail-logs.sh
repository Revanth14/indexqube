#!/usr/bin/env bash
set -euo pipefail

journalctl -u indexqube-gateway -f -o cat
