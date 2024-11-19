#!/usr/bin/env python3

import boto3

s3 = boto3.client("s3")


def boto3_tags_to_dict(tags):
    boto3_dict = {}
    for tag in tags:
        key = tag.get("Key", None)
        if key:
            boto3_dict[key] = tag.get("Value", None)
    return boto3_dict


def dict_to_boto3_tags(boto3_dict):
    tags = []
    for key in boto3_dict:
        tags.append({"Key": key, "Value": boto3_dict[key]})
    return tags


def get_tag_dict(bucket, key):
    response = s3.get_object_tagging(Bucket=bucket, Key=key)
    return boto3_tags_to_dict(response.get("TagSet", []))


def update_tags(bucket, key, new_tag_dict):
    tags = dict_to_boto3_tags(new_tag_dict)

    response = s3.put_object_tagging(Bucket=bucket, Key=key, Tagging={"TagSet": tags})
    return response


def merge_and_update_tags(bucket, key, update_tag_dict):
    revised_tag_dict = get_tag_dict(bucket,key).copy()
    revised_tag_dict.update(update_tag_dict)
    return update_tags(bucket, key, revised_tag_dict)
