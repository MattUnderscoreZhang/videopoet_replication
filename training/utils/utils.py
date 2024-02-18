from typing import Literal


HIDE_CURSOR = "\033[?25l"  # ]
SHOW_CURSOR = "\033[?25h"  # ]
LINE_CLEAR = "\x1b[2K"  # ]


def print_progress(message: str, status: Literal["yellow", "green", "red"]) -> None:
    print(HIDE_CURSOR, end="")
    if status == "yellow":
        yellow_circle = "\033[33m\u25CF\033[0m"  # ]]
        print(yellow_circle + " | " + message, end="\r")
    else:
        red_circle = "\033[31m\u25CF\033[0m"  # ]]
        green_circle = "\033[32m\u25CF\033[0m"  # ]]
        circle = red_circle if status == "red" else green_circle
        print(end=LINE_CLEAR)
        print(circle + " | " + message)
    print(SHOW_CURSOR, end="")
