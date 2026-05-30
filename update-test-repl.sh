#!/bin/bash
# This file runs the tests

SCRIPTPATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
DIRNAME="$(dirname ${SCRIPTPATH})"

REPL_TEST_DIR="${DIRNAME}/noise-tests/repl-tests"
cargo build --example repl-rocksdb -p noise-storage-rocksdb
exit_status=$?
if [ $exit_status -ne 0 ]; then
  exit $exit_status
fi
NOISE="${DIRNAME}/target/debug/examples/repl-rocksdb"
if [[ ! -f "${NOISE}" ]]; then
  echo "Can't find repl-rocksdb example, looked at ${NOISE}"
  exit 1
fi

REPL_TESTS="${REPL_TEST_DIR}/*.noise"
for f in $REPL_TESTS
do
  echo -n "Testing: ${f}..."
  RUST_BACKTRACE=1 "${NOISE}" -t < "${f}" > "${f}.out"
  echo "updating."
  cp "${f}.out" "${f}"
  rm "${f}.out"
done

echo "Updated tests. Use \`\`git diff ./noise-tests/repl-tests\`\` to review the changes."
