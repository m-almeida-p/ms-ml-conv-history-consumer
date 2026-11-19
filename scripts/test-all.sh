#!/usr/bin/env bash
# Runs every test of the repo, one suite after another, and ends with one table.
#
#   ./scripts/test-all.sh               docs check, JUnit, and the consumer against the local cluster
#   ./scripts/test-all.sh --only camel  only the named suites (names are in the SUITES list below)
#   ./scripts/test-all.sh --stop        stop at the first suite that fails (the default runs them all)
#
# Needs: Docker, the cluster up (Kafka, Postgres, Schema Registry), python3.
# The full output of each suite is kept in .test-logs/<name>.log (git-ignored); the table shows its last summary line.
# Every suite removes what it created (rows, topics), so they can run again at once.

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
require_docker
require_cluster_network

# name | command
SUITES=(
  "docs|python3 -I scripts/check-docs.py"
  "camel|scripts/test-camel.sh"
  "consumer|scripts/test-consumer.sh"
)

stop=0; only=""
while [ $# -gt 0 ]; do
  case "$1" in
    --stop) stop=1 ;;
    --only) shift; only="$1" ;;
    -h|--help) sed -n 2,8p "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1 (see --help)" >&2; exit 2 ;;
  esac
  shift
done

LOGS="${REPO_DIR}/.test-logs"
mkdir -p "${LOGS}"
cd "${REPO_DIR}"

names=(); results=(); times=(); notes=()
failed=0
for entry in "${SUITES[@]}"; do
  IFS='|' read -r name command <<< "${entry}"
  if [ -n "${only}" ] && ! [[ ",${only}," == *",${name},"* ]]; then continue; fi
  names+=("${name}"); log="${LOGS}/${name}.log"
  echo "▶  ${name}: ${command}"
  start=$(date +%s)
  if bash -c "${command}" > "${log}" 2>&1; then status="ok"; else status="FAILED"; failed=$((failed + 1)); fi
  seconds=$(( $(date +%s) - start ))
  summary="$(sed 's/\x1b\[[0-9;]*m//g' "${log}" | grep -aE 'tests,|checks|all checks passed|agree|passed|✅|❌' | tail -1 | cut -c1-90)"
  results+=("${status}"); times+=("${seconds}s"); notes+=("${summary}")
  if [ "${status}" = FAILED ]; then
    echo "   ❌ ${name} failed; last lines of ${log}:"; sed 's/\x1b\[[0-9;]*m//g' "${log}" | grep -E 'FAIL|ERROR|expected|❌' | head -12 | cut -c1-200 | sed 's/^/      /'
    [ "${stop}" = 1 ] && break
  fi
done

echo
printf '%-10s %-8s %6s   %s\n' suite result time "last line"
printf '%-10s %-8s %6s   %s\n' ---------- -------- ------ ---------
for i in "${!names[@]}"; do printf '%-10s %-8s %6s   %s\n' "${names[$i]}" "${results[$i]}" "${times[$i]}" "${notes[$i]}"; done
echo
if [ "${failed}" = 0 ]; then echo "✅  every suite passed"; else echo "❌  ${failed} suite(s) failed (full output in .test-logs/)"; exit 1; fi
