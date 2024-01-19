rm -rf videos/processed/webvid_sample/
source process_videos.sh webvid_sample
cd videos/processed/webvid_sample/
dtrx 00000.tar
cd -
python scripts/check_video_frames.py
