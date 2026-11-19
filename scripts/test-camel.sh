#!/usr/bin/env bash
# Runs the JUnit and Camel tests of the projects (tests/camel/<project>) in a Maven container on the cluster network.
#
#   ./scripts/test-camel.sh                 every test of the consumer
#   ./scripts/test-camel.sh consumer DecoderTest    one test class
#
# The module compiles the project's OWN Java files, so a test breaks when the code does. The route tests use the cluster's Postgres
# (the tables of the schema history; rows prefixed 'jt-', removed after each test). Needs: Docker and the cluster up. The Maven cache is the
# volume camel-tests-m2, so only the first run downloads dependencies.

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
require_docker
require_cluster_network
modules=("${1:-consumer}")
filter="${2:-}"
status=0
for m in "${modules[@]}"; do
  [ -f "${REPO_DIR}/tests/camel/${m}/pom.xml" ] || { echo "no module tests/camel/${m}"; continue; }
  echo "== camel tests: ${m}"
  rm -rf "${REPO_DIR}/tests/camel/${m}/target/surefire-reports"   # a report of a test that no longer exists must not be counted
  docker run --rm --network yape-local -v "${REPO_DIR}:/work" -v camel-tests-m2:/root/.m2 -w "/work/tests/camel/${m}" \
    -e JDBC_URL="jdbc:postgresql://db:5432/postgres?currentSchema=history" \
    maven:3.9-eclipse-temurin-21 mvn -q -B ${filter:+-Dtest="${filter}"} -Dsurefire.failIfNoSpecifiedTests=false test 2>&1 | tail -60 || status=1
  [ "${PIPESTATUS[0]}" = 0 ] || status=1
  # surefire writes a summary per class; show the totals
  python3 -I - "${REPO_DIR}/tests/camel/${m}/target/surefire-reports" <<'PY'
import sys, glob, xml.etree.ElementTree as ET
tests = fails = errs = skipped = 0
for f in glob.glob(sys.argv[1] + "/TEST-*.xml"):
    r = ET.parse(f).getroot()
    tests += int(r.get("tests", 0)); fails += int(r.get("failures", 0)); errs += int(r.get("errors", 0)); skipped += int(r.get("skipped", 0))
print(f"   {tests} tests, {fails} failed, {errs} errors, {skipped} skipped")
PY
done
exit "${status}"
