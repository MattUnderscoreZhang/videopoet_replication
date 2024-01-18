#!/bin/bash

if [ "$1" = "webvid" ]; then
    video2dataset --url_list="urls/webvid/results_2M_val.csv" \
        --input_format="csv" \
        --output-format="webdataset" \
        --output_folder="videos/unprocessed/webvid" \
        --url_col="contentUrl" \
        --caption_col="name" \
        --save_additional_columns='[videoid,page_idx,page_dir,duration]' \
        --enable_wandb=True \
        --config=default
elif [ "$1" = "webvid_sample" ]; then
    video2dataset --url_list="urls/webvid/results_2M_val_sample.csv" \
        --input_format="csv" \
        --output-format="webdataset" \
        --output_folder="videos/unprocessed/webvid_sample" \
        --url_col="contentUrl" \
        --caption_col="name" \
        --save_additional_columns='[videoid,page_idx,page_dir,duration]' \
        --enable_wandb=False \
        --config=default
else
    echo "Unknown argument $1"
fi
