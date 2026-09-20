#!/bin/bash
set -eu

APPDEST="${TRIM_APPDEST:-/var/apps/music-mate/target}"
PKGVAR="${TRIM_PKGVAR:-/var/apps/music-mate/var}"
DOCKER_DIR="${APPDEST}/docker"
if [ ! -d "$DOCKER_DIR" ] && [ -d "${APPDEST}/app/docker" ]; then
  DOCKER_DIR="${APPDEST}/app/docker"
fi
AUTH_FILE="${PKGVAR}/authorized-paths"
OVERRIDE_FILE="${DOCKER_DIR}/docker-compose.override.yaml"
COMPOSE_FILE="${DOCKER_DIR}/docker-compose.yaml"

mkdir -p "${PKGVAR}"

# 1. Parse TRIM_DATA_ACCESSIBLE_PATHS into array
RAW_PATHS="${TRIM_DATA_ACCESSIBLE_PATHS:-}"
CLEAN_PATHS=()

if [ -n "$RAW_PATHS" ]; then
  IFS=':' read -r -a PATH_ARRAY <<< "$RAW_PATHS"
  for p in "${PATH_ARRAY[@]}"; do
    p="$(echo "$p" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's#/*$##')"
    if [ -n "$p" ]; then
      CLEAN_PATHS+=("$p")
    fi
  done
fi

# 2. Save authorized paths to PKGVAR for container reading
TMP_AUTH="${AUTH_FILE}.tmp.$$"
: > "$TMP_AUTH"
for p in "${CLEAN_PATHS[@]}"; do
  echo "$p" >> "$TMP_AUTH"
done
mv -f "$TMP_AUTH" "$AUTH_FILE"
chmod 666 "$AUTH_FILE" 2>/dev/null || true

# 3. Generate docker-compose.override.yaml
if [ -d "$DOCKER_DIR" ]; then
  TMP_OVERRIDE="${OVERRIDE_FILE}.tmp.$$"
  {
    echo "services:"
    echo "  organizer:"
    echo "    environment:"
    echo "      TRIM_DATA_ACCESSIBLE_PATHS: \"${RAW_PATHS}\""
    if [ "${#CLEAN_PATHS[@]}" -gt 0 ]; then
      echo "    volumes:"
      for p in "${CLEAN_PATHS[@]}"; do
        echo "      - \"${p}:${p}:rw\""
      done
    fi
  } > "$TMP_OVERRIDE"
  mv -f "$TMP_OVERRIDE" "$OVERRIDE_FILE"

  # 4. Trigger docker compose up to apply mounts dynamically
  if [ -f "$COMPOSE_FILE" ] && command -v docker >/dev/null 2>&1; then
    docker compose -p music-mate -f "$COMPOSE_FILE" -f "$OVERRIDE_FILE" up -d --remove-orphans >/dev/null 2>&1 || true
  fi
fi

exit 0
