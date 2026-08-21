# Generates TypeScript stubs from crates/palisade-proto/proto/palisade.v1.proto.
# Requires: npm install -g protobuf-ts-cli  (or npx)
#
#   ./scripts/gen-stubs-typescript.sh

set -euo pipefail
PROTO="crates/palisade-proto/proto/palisade.v1.proto"
OUT="bindings/typescript/palisade"
mkdir -p "$OUT"
npx protoc-ts-plugin -I"$(dirname "$PROTO")" --out="$OUT" "$(basename "$PROTO")" ||
  npx protoc --plugin=protoc-gen-ts_proto="$(npx -y ts-proto/protoc-gen-ts_proto)" \
    -I"$(dirname "$PROTO")" --ts_proto_out="$OUT" "$(basename "$PROTO")"
echo "stubs written to $OUT"
