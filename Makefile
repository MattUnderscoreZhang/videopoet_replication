#!/usr/bin/env make

download_videos:
	#python -m data.youtube_download
	video2dataset --url_list="data/video_urls/old_cartoons_sample.csv" --url_col="url" --caption_col="caption" --output_folder="data/videos/old_cartoons_sample"

train_tokenizer:
	python -m tokenizer.tokenizer_training
