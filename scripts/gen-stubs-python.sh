# Generates Python stubs from crates/palisade-proto/proto/palisade.v1.proto.
# Requires: pip install grpcio-tools
#
#   python -m pip install grpcio-tools
#   ./scripts/gen-stubs-python.sh

set -euo pipefail
PROTO="crates/palisade-proto/proto/palisade.v1.proto"
OUT="bindings/python/palisade"
mkdir -p "$OUT"
python -m grpc_tools.protoc -I"$(dirname "$PROTO")" \
  --python_out="$OUT" --grpc_python_out="$OUT" --pyi_out="$OUT" \
  "$(basename "$PROTO")"
touch "$OUT/__init__.py"
echo "stubs written to $OUT (import as: from palisade import palisade_v1_pb2, palisade_v1_pb2_grpc)"
