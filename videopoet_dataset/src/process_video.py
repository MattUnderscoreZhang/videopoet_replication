import ffmpeg
from math import floor

from .video_metadata import get_scene_start_times


def process_video(input_filepath: str, segment_time: float, output_folder: str) -> None:
    """
    Extracts video fragments from a given video file based on scene changes, segment time, and saves them to a specified output folder.

    This function takes an input video file, a segment time, and an output folder, then extracts fragments of the video
    based on the scene start times. Each fragment is processed to a fixed frame rate and resolution,
    and then saved as a series of clips in the specified output folder.

    Parameters:
    - input_filepath (str): The path to the input video file.
    - segment_time (float): The desired time duration (in seconds) of each video segment.
    - output_folder (str): The path to the folder where the output video clips will be saved.

    The function does not return any value but saves the processed video clips to the disk.
    """
    video_duration = ffmpeg.probe(input_filepath)['format']['duration']
    scene_times = get_scene_start_times(input_filepath) + [video_duration]

    # Extract and process clips of segment_time from each scene, discarding the remainder.
    for scene_i, (ss, to) in enumerate(zip(scene_times[:-1], scene_times[1:])):
        to = ss + floor((to - ss) / segment_time) * segment_time
        (
            ffmpeg.input(input_filepath, ss=ss, to=to)
            .filter('fps', fps=8)
            .filter('scale', -1, 224)
            .filter('crop', w=128, h=224)
            .output(
                f"{output_folder}/output_clip_{scene_i}_%d.mp4",
                codec='libx264',  # Use the h.264 codec
                f='segment',  # Use the segment format
                segment_time=str(segment_time),  # Segment duration in seconds
                reset_timestamps=1,  # Reset timestamps at the beginning of each segment
                force_key_frames=f'expr:gte(t,n_forced*{segment_time})',  # Force key frames at segment times
            ).run(capture_stdout=True, quiet=True)
        )