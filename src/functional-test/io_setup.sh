#!/usr/bin/env bash

# Usage:
# --output_remote_dir

# Set args defaults
OUTPUT_REMOTE="s3://mybucket/output/"

# Read params
echo 'Reading script params...'
#while [ $# -gt 0 ]; do
for i in "$@"
do
case $i in
--output_remote_dir=*)
OUTPUT_REMOTE="${i#*=}"
shift
;;
-*)
# do not exit out, just note failure
echo "unrecognized option: ${i#*=}"
;;
*)
break;
;;
esac
shift
done
echo 'Running with parameters : '
echo "OUTPUT_REMOTE = ${OUTPUT_REMOTE}"

echo "Preparing FS"
aws s3 rm --recursive $OUTPUT_REMOTE
