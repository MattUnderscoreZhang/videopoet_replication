#!/bin/bash

if [ "$1" = "webvid" ]; then
    video2dataset --url_list="urls/webvid/results_2M_val.csv" \
        --input_format="csv" \
        --output-format="webdataset" \
        --output_folder="videos/processed/webvid" \
        --url_col="contentUrl" \
        --caption_col="name" \
        --save_additional_columns='[videoid,page_idx,page_dir,duration]' \
        --enable_wandb=False \
        --config="configs/webvid.yaml"
elif [ "$1" = "webvid_sample" ]; then
    video2dataset --url_list="urls/webvid/results_2M_val_sample.csv" \
        --input_format="csv" \
        --output-format="webdataset" \
        --output_folder="videos/processed/webvid_sample" \
        --url_col="contentUrl" \
        --caption_col="name" \
        --save_additional_columns='[videoid,page_idx,page_dir,duration]' \
        --enable_wandb=False \
        --config="configs/webvid_mini.yaml"
elif [ "$1" = "webvid_time_test" ]; then
    video2dataset --url_list="urls/webvid/results_2M_val_time_test.csv" \
        --input_format="csv" \
        --output-format="webdataset" \
        --output_folder="videos/processed/webvid_time_test" \
        --url_col="contentUrl" \
        --caption_col="name" \
        --save_additional_columns='[videoid,page_idx,page_dir,duration]' \
        --enable_wandb=True \
        --config="configs/webvid_mini.yaml"
else
    echo "Unknown argument $1"
fi
