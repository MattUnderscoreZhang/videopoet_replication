import ffmpeg
from scenedetect import ContentDetector, SceneManager, open_video


def get_keyframe_times(video_filepath: str) -> list[float]:
    """
    Extracts keyframe times from a video file using ffmpeg-python.

    Args:
    - video_filepath: The path to the video file.

    Returns:
    - A list of floats representing the keyframe times in the video.
    """
    video_metadata = ffmpeg.probe(
        video_filepath,
        select_streams='v',
        show_entries='packet=pts_time,flags',
    )
    keyframe_timestamps = [
        float(packet["pts_time"])
        for packet in video_metadata["packets"]
        if "K" in packet.get("flags", "")
    ]
    return keyframe_timestamps


def get_scene_start_times(video_filepath: str) -> list[float]:
    """
    Uses scenedetect to find scene start times in a video file.

    Args:
    - video_filepath: The path to the video file.

    Returns:
    - A list of floats representing the times at which scene transitions occur.
    """
    # Create a video manager object for the video.
    video_manager = open_video(video_filepath)

    # Create a SceneManager and add a ContentDetector algorithm.
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector())

    # Detect scenes in the video.
    scene_manager.detect_scenes(video_manager)

    # Get the list of scenes from the scene manager.
    scene_list = scene_manager.get_scene_list()

    # Extract the start times of each scene.
    scene_start_times = [scene[0].get_seconds() for scene in scene_list]

    return scene_start_times