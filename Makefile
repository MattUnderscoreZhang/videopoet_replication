#!/usr/bin/env make

download_videos:
	python -m tokenizer.youtube_download

train_tokenizer:
	python -m tokenizer.tokenizer_training
