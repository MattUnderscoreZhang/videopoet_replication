import pyjson5 as json
from pytube import YouTube

from utils.utils import print_progress


if __name__ == "__main__":
    dataset = "old_cartoons_test"
    with open(f"data/video_urls/{dataset}.json") as f:
        videos = json.load(f)
    for video_url in videos:
        print_progress(f"Downloading {video_url}", "yellow")
        yt = YouTube(video_url)
        # all_streams = yt.streams.all()
        video = yt.streams.get_highest_resolution()
        assert video is not None
        video.download(f"data/videos/{dataset}")
        print_progress(f"Downloaded {video_url}", "green")
