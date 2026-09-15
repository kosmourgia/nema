#!/usr/bin/env bash
set -euo pipefail
module=$(cd -- "$(dirname -- "$0")/.." && pwd)
deps="$module/.nema/deps"
mkdir -p "$deps" "$module/.nema/release-source"
source_jar="$deps/aeron-all-1.53.1-sources.jar"
if [[ ! -f "$source_jar" ]]; then
    curl --fail --location --retry 2 https://repo.maven.apache.org/maven2/io/aeron/aeron-all/1.53.1/aeron-all-1.53.1-sources.jar -o "$source_jar"
fi
actual=$(sha256sum "$source_jar")
[[ ${actual%% *} == 27af40c95139b945e3476b520eacc7cddb62f12917deccf6114529463e82e9a1 ]]
for path in io/aeron/Publication.java io/aeron/ControlledFragmentAssembler.java io/aeron/BufferBuilder.java \
    io/aeron/logbuffer/Header.java io/aeron/archive/client/PersistentSubscription.java \
    io/aeron/archive/client/AeronArchive.java io/aeron/archive/RecordingWriter.java; do
    target="$module/.nema/release-source/$path"
    mkdir -p "$(dirname -- "$target")"
    unzip -p "$source_jar" "$path" >"$target"
done
echo "Pinned source excerpts retained: $module/.nema/release-source"
