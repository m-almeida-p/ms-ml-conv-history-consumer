#!/usr/bin/env bash
# Runs a Karavan project folder of this repo in the DevMode image, on the cluster network, WITHOUT Karavan.
# It is the same runtime Karavan's Run button uses (ghcr.io/apache/camel-karavan-devmode), fed from the files
# in the repo instead of Karavan's database, so you can edit, run and read logs in seconds.
#
#   ./scripts/devmode-local.sh <project-folder> [NAME=value ...]   start (replaces a running copy)
#   ./scripts/devmode-local.sh stop <project-folder>               stop and remove it
#
# NAME=value pairs become environment variables of the container. The app.* settings of the project's
# application.properties can be overridden that way: app.topic is APP_TOPIC, app.kafka.groupId is
# APP_KAFKA_GROUPID. Example, a throwaway topic and group:
#   ./scripts/devmode-local.sh conv-history-consumer APP_TOPIC=camel.test.events APP_KAFKA_GROUPID=camel-test
#
# Logs:  docker logs -f devmode-local-<project-folder>
# The Maven cache is kept in the volume devmode-local-m2, so only the first start downloads dependencies.
# The log starts with a "Fetching project null files from Platform" ConnectException. It is harmless: the image
# tries to download the project from Karavan first, and with no Karavan project id it falls back to the files
# in the folder it was given.
# This is a development aid. Use Karavan (the Run button at the top right of the project) to run the project the normal way.

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

IMAGE="${DEVMODE_IMAGE:-ghcr.io/apache/camel-karavan-devmode:4.22.2}"

if [ "${1:-}" = "stop" ]; then
  shift
  project="${1:?usage: $0 stop <project-folder>}"
  docker rm -f "devmode-local-${project}" >/dev/null 2>&1 && echo "Stopped devmode-local-${project}" || echo "devmode-local-${project} is not running"
  exit 0
fi

project="${1:?usage: $0 <project-folder> [NAME=value ...]}"
shift
src="${REPO_DIR}/${project}"
[ -d "${src}" ] || { echo "❌  ${src} is not a folder" >&2; exit 1; }

require_docker
require_cluster_network

name="devmode-local-${project}"
# The container writes into its code folder (.camel-jbang), so it gets a copy, not the repo folder. The copy
# lives inside the repo (git-ignored) because the system temp folder is not shared with the Docker VM.
work="${REPO_DIR}/.devmode-local/${project}"
rm -rf "${work}"
mkdir -p "${work}"
cp -R "${src}/." "${work}/"
# Karavan's files for its Deploy feature, not routes: Karavan does not hand them to DevMode either.
rm -f "${work}/docker-compose.yaml" "${work}/PROJECT.md"
chmod -R a+rwX "${work}"

env_args=()
for pair in "$@"; do env_args+=(-e "${pair}"); done

docker rm -f "${name}" >/dev/null 2>&1 || true
docker run -d --name "${name}" --network yape-local \
  -v "${work}:/karavan/code" \
  -v devmode-local-m2:/karavan/.m2 \
  -e CODE_DIR=/karavan/code \
  ${env_args[@]+"${env_args[@]}"} \
  --entrypoint bash "${IMAGE}" \
  -c 'java $JAVA_OPTS -cp "$KARAVAN_LIB/*" KaravanDevMode run --source-dir="$CODE_DIR" --logging-color=false --console' >/dev/null

echo "🚀  ${name} started from ${src}"
echo "    logs:  docker logs -f ${name}"
echo "    stop:  $0 stop ${project}"
