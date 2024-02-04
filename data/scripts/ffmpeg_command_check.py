import ffmpeg
import json
from math import floor
import subprocess


def get_keyframe_times(video_filepath):
    command = [
        "ffprobe",
        "-v", "quiet",  # Suppress the output of the program
        "-select_streams", "v",  # Select only the video stream
        "-show_entries", "packet=pts_time,flags",  # Show the key frame and the presentation time stamp
        "-of", "json",  # Output in JSON format
        video_filepath,
    ]
    process = subprocess.run(command, capture_output=True, text=True)
    video_metadata = json.loads(process.stdout)
    keyframe_timestamps = [
        float(packet["pts_time"]) for packet in video_metadata["packets"] if "K" in packet.get("flags", "")
    ]
    # keyframe_times = [float(frame["pkt_pts_time"]) for frame in frames if frame["key_frame"] == 1]
    breakpoint()


# scene_transition_times = [0.0, 13.0, 17.0]
scene_transition_times = get_keyframe_times("videos/processed/webvid/00000/0000000.mp4")
segment_time = 2.215


for scene_i, (ss, to) in enumerate(zip(scene_transition_times[:-1], scene_transition_times[1:])):
    to = ss + floor((to - ss) / segment_time) * segment_time
    (
        # ffmpeg.input("videos/processed/webvid/00000/0000000.mp4")
        ffmpeg.input("videos/processed/webvid/00000/0000000.mp4", ss=ss, to=to)
        .filter('fps', fps=8)
        .filter('scale', -1, 224)
        .filter('crop', w=128, h=224)
        .output(
            f"videos/processed/webvid/00000/clips/output_clip_{scene_i}_%d.mp4",
            codec='libx264',  # Use the h.264 codec
            f='segment',  # Use the segment format
            segment_time=str(segment_time),  # Segment duration in seconds
            reset_timestamps=1,  # Reset timestamps at the beginning of each segment
            force_key_frames=f'expr:gte(t,n_forced*{segment_time})',  # Force key frames at segment times
        ).run(capture_stdout=True, quiet=True)
    )
