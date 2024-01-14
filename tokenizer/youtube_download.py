import pyjson5 as json
from pytube import YouTube

from .utils import print_progress


if __name__ == "__main__":
    with open("tokenizer/videos.json") as f:
        videos = json.load(f)
    # video_url = "https://www.youtube.com/watch?v=57jZv07GmI0"
    for video_url in videos:
        print_progress(f"Downloading {video_url}", "yellow")
        yt = YouTube(video_url)
        # all_streams = yt.streams.all()
        video = yt.streams.get_highest_resolution()
        video.download("tokenizer/data/videos")
        print_progress(f"Downloaded {video_url}", "green")
