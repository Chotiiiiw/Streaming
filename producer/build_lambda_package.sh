#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
build_root="${script_dir}/build"
package_dir="${build_root}/lambda"
package_path="${build_root}/lambda-producer.zip"

case "${package_dir}" in
  "${script_dir}/build/lambda") ;;
  *)
    echo "Refusing to clean unexpected package directory: ${package_dir}" >&2
    exit 1
    ;;
esac

rm -rf "${package_dir}"
rm -f "${package_path}"
mkdir -p "${package_dir}"

python3 -m pip install \
  --no-compile \
  --requirement "${script_dir}/requirements-lambda.txt" \
  --target "${package_dir}"

cp "${script_dir}/lambda_handler.py" "${package_dir}/"
cp "${script_dir}/transaction_generator.py" "${package_dir}/"

(
  cd "${package_dir}"
  zip -q -r "${package_path}" .
)

echo "Created ${package_path}"
