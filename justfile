set shell := ["bash", "-c"]

COMPOSE_FILE := "test-resources/docker-compose.yaml"
ENV_FILE := "test-resources/integration-tests.env"

# List available recipes
default:
    @just --list

build:
    sbt assembly

docker-build version="dev":
    docker build \
        --platform linux/amd64,linux/arm64 \
        --build-arg GITHUB_TOKEN \
        -t ghcr.io/sneaksanddata/arcane-stream-json:{{version}} \
        .

clean:
    sbt clean

# Start test services in background
up *args="":
    docker compose -f {{COMPOSE_FILE}} up -d {{args}}

# Stop test services
stop *args="":
    docker compose -f {{COMPOSE_FILE}} down {{args}}

# Restart test services
fresh *args="":
    docker compose -f {{COMPOSE_FILE}} restart {{args}}

# View logs for test services
logs *args="":
    docker compose -f {{COMPOSE_FILE}} logs {{args}}

# View test services status
ps:
    docker compose -f {{COMPOSE_FILE}} ps

# NOTE: defaults to Test/testFull because under sbt 2 a bare `test` delegates to
# `testQuick`, which silently selects no tests and still exits successfully,
# hiding integration test failures.
# Run tests with integration-tests.env loaded
test *args="Test/testFull":
    export $(grep -v '^#' {{ENV_FILE}} | xargs) && sbt "{{args}}"

# Accepts a bare class name (StreamRunner), an explicit glob (*StreamRunner*),
# or a direct fully-qualified test address (com.sneaksanddata...StreamRunner).
# Run specific tests with integration-tests.env loaded
test-only *args:
    #!/usr/bin/env bash
    set -euo pipefail
    export $(grep -v '^#' {{ENV_FILE}} | xargs)
    ARGS="{{args}}"
    if [[ "$ARGS" == *"*"* ]] || [[ "$ARGS" == *" "* ]] || [[ "$ARGS" == *"."* ]]; then
        # explicit glob, multiple selectors, or direct test address: pass through as-is
        sbt "Test/testOnly ${ARGS}"
    else
        # bare class name: match it anywhere in the fully-qualified name
        sbt "Test/testOnly *${ARGS}*"
    fi

# Validate formatting
fmt-check:
    sbt scalafmtCheckAll

# Format code
fmt:
    sbt scalafmtAll

check: fmt-check

it: build
    #!/usr/bin/env bash
    set -euo pipefail

    # cleanup docker regardless of test outcome
    trap 'just stop' EXIT

    just up
    docker compose -f {{COMPOSE_FILE}} wait s3_reader_prepare lakekeeper_migrate lakekeeper_prepare
    just test

stream debug="":
    #!/usr/bin/env bash
    set -euo pipefail

    if [ ! -f "dev.env" ]; then
        echo "Error: dev.env file not found."
        echo "Please copy dev.env.example to dev.env and configure it."
        exit 1
    fi

    DEBUG_OPTS=""
    if [ "{{debug}}" != "" ]; then
        DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"
        echo "Debug mode enabled. Listening on port 5005..."
    fi

    JAR_FILE="target/com.sneaksanddata.arcane.stream-json.assembly.jar"
    if [ ! -f "$JAR_FILE" ]; then
        echo "Assembly JAR not found. Running 'just build' first..."
        just build
    fi

    echo "Running Stream..."
    mise exec --env dev -- java $DEBUG_OPTS -jar "$JAR_FILE"

backfill debug="":
    STREAMCONTEXT__BACKFILL="true" just stream "{{debug}}"
