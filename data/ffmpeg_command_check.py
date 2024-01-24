import ffmpeg


if __name__ == "__main__":
    clip_times = "2.125,4.25,6.375,8.5,10.625,12.75,14.875"
    try:
        (
            ffmpeg.input("videos/unprocessed/00000/00000003.mp4")
            .filter("fps", fps=8)
            .filter("scale", -2, 224)
            .filter("crop", w=128, h=224)
            .filter("pad", w=128, h=224)
            .output(
                "clip_%d.mp4",
                **{
                    "map": 0,
                    "f": "segment",
                    "segment_times": clip_times,
                    "reset_timestamps": 1,
                    "force_key_frames": clip_times,
                }
            )
            .run(capture_stdout=True, quiet=True)
        )
    except Exception as err:
        print(err.stderr)
        breakpoint()
