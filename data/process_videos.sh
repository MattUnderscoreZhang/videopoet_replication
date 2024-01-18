#!/bin/bash

if [ "$1" = "webvid" ]; then
    video2dataset --url_list="videos/unprocessed/webvid/{00000..00004}.tar" \
        --input_format="webdataset" \
        --output-format="webdataset" \
        --output_folder="videos/processed/webvid" \
        --stage "subset" \
        --config "configs/process_webvid.yaml"
elif [ "$1" = "webvid_sample" ]; then
    video2dataset --url_list="videos/unprocessed/webvid_sample/00000.tar" \
        --input_format="webdataset" \
        --output-format="webdataset" \
        --output_folder="videos/processed/webvid_sample" \
        --stage "subset" \
        --config "configs/process_webvid.yaml"
else
    echo "Unknown argument $1"
fi
