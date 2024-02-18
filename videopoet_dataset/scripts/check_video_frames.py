from pathlib import Path
import cv2


dir = Path("/Users/matt/Projects/video_poet_replication/data/videos/processed/webvid_sample/00000/")
total_frames = []
for file in dir.glob("*.mp4"):
    vid = cv2.VideoCapture(str(file))
    total_frames.append(int(vid.get(cv2.CAP_PROP_FRAME_COUNT)))
    vid.release()
print(total_frames)
