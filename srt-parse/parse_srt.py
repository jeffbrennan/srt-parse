from pathlib import Path
import time
from typing import TypedDict

class SrtLine(TypedDict):
    index: int
    start: str
    end: str
    text: str


srt_files = Path("srt").glob("**/*.srt")
print(srt_files)


for srt_file in srt_files:
    with open(srt_file, "r") as f:
        srt_raw = f.read()

    parsed_srt = srt_raw.splitlines()
    parsed_srt = [i for i in parsed_srt if i != ""]
    parsed_results: list[SrtLine] = []
    print("parsing srt file")
    time.sleep(1)
    for i in range(0, len(parsed_srt), 3):
        parsed_result: SrtLine = {
            "index": int(parsed_srt[i]),
            "start": parsed_srt[i+1].split(" --> ")[0],
            "end": parsed_srt[i+1].split(" --> ")[1],
            "text": parsed_srt[i+2]
        }
        print(parsed_result)
        parsed_results.append(parsed_result)
