#!/bin/bash

funcname="${1}"

if [ x"$funcname" = 'x' ]; then
    echo "usage: ${0} <funcname>"
    exit 1
fi

outf=$(mktemp)
aws lambda invoke --function-name "$1" "$outf"
echo 'Lambda function output:'
echo '-----------------------'
cat "$outf" && rm -f outf
echo
echo '-----------------------'
