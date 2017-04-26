#!/bin/bash
# This file runs the tests 

SCRIPTPATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
DIRNAME="$(dirname ${SCRIPTPATH})"

NOISE="${DIRNAME}/target/debug/noise_search"
REPL_TEST_DIR="${DIRNAME}/repl-tests"

if [[ ! -f "${NOISE}" ]]; then
  echo "Can't find noise binary, looked at ${NOISE}"
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

echo "Updated tests. Use \`\`git diff ./repl-tests\`\` to review the changes."
