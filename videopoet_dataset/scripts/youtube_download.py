import cv2
import numpy as np
from pathlib import Path
import pyjson5 as json
from pytube import YouTube
from io import BytesIO

from utils.utils import print_progress


def downscale_video(video_clip: BytesIO, target_width: int, target_height: int):
    video_array = np.frombuffer(video_clip.getvalue(), dtype=np.uint8)
    frame_array = cv2.imdecode(video_array, cv2.IMREAD_COLOR)

    breakpoint()
    resized_frame = cv2.resize(frame_array, (target_width, target_height))

    crop_x = (resized_frame.shape[1] - target_width) // 2
    crop_y = (resized_frame.shape[0] - target_height) // 2
    cropped_frame = resized_frame[crop_y:crop_y + target_height, crop_x:crop_x + target_width]

    _, processed_content = cv2.imencode('.mp4', cropped_frame)
    return BytesIO(processed_content.tobytes())


def download_videos(video_ids: list[str], target_width: int, target_height: int, save_dir: Path):
    for video_id in video_ids:
        video_url = "https://www.youtube.com/watch?v=" + video_id
        print_progress(f"Downloading {video_url}", "yellow")
        
        try:
            youtube = YouTube(video_url)
            video_stream = youtube.streams.get_lowest_resolution()
            assert video_stream is not None
            video_buffer = BytesIO()
            video_stream.stream_to_buffer(video_buffer)
        except Exception:
            print_progress(f"Could not download video {video_url}", "red")
            continue
        
        processed_clip = downscale_video(video_buffer, target_width, target_height)
        output_video_path = save_dir / "{video_id}.mp4"
        breakpoint()
        processed_clip.write_videofile(output_video_path, codec='libx264', audio_codec='aac')

        print_progress(f"Processed {video_url}", "green")


if __name__ == "__main__":
    dataset = "old_cartoons_test"
    with open(f"data/video_ids/{dataset}.json") as f:
        video_ids = json.load(f)
    download_videos(video_ids, target_width=128, target_height=128, save_dir=Path(f"data/videos/{dataset}"))
