#!/bin/bash

CCG_PROJECT=$(./develop.sh ccg-project | tr -d '\r')
BUILD_VERSION=$(./develop.sh build-version | tr -d '\r')

if [ x"$BUILD_VERSION" = x ]; then
    echo "$BUILD_VERSION could not be determined, aborting."
    exit 1
fi

FILENAME="${CCG_PROJECT}-${BUILD_VERSION}.zip"
ZIPFILE="./build/${FILENAME}"
aws s3 cp "$ZIPFILE" s3://bpa-lambda

# this blob of code may have multiple handler functions in it; update each of them
for funcname in BPAMAdminCSVExport CKANCheckAllResourcesIntegrities CKANResourceIntegrityCheck SpreadsheetFunc; do
    aws lambda update-function-code --function-name "$funcname" --s3-bucket bpa-lambda --s3-key "${FILENAME}"
done

