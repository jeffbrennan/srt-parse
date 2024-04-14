from pathlib import Path
from typing import TypedDict
import polars as pl


class SrtLine(TypedDict):
    path: Path
    index: int
    start: str
    end: str
    text: str


def parse_srt(path: Path) -> list[SrtLine]:
    with path.open("r") as f:
        srt = f.read()
    parsed_srt = srt.splitlines()
    parsed_srt = [i for i in parsed_srt if i != ""]
    parsed_results: list[SrtLine] = []
    for i in range(0, len(parsed_srt), 3):
        parsed_result: SrtLine = {
            "path": path,
            "index": int(parsed_srt[i]),
            "start": parsed_srt[i + 1].split(" --> ")[0],
            "end": parsed_srt[i + 1].split(" --> ")[1],
            "text": parsed_srt[i + 2],
        }
        parsed_results.append(parsed_result)
    return parsed_results


def clean_srt(lines: list[SrtLine]) -> pl.DataFrame:
    MIN_WORDS_PER_SECOND = 1.5
    MIN_WORDS_PER_CHUNK = 300

    df = pl.DataFrame(lines)
    df_clean = (
        df.with_columns(
            [
                pl.concat_str([pl.lit("2024-01-01T"), pl.col("start")])
                .str.to_datetime("%Y-%m-%dT%H:%M:%S,%f", strict=True)
                .alias("start"),
                pl.concat_str([pl.lit("2024-01-01T"), pl.col("end")])
                .str.to_datetime("%Y-%m-%dT%H:%M:%S,%f", strict=True)
                .alias("end"),
            ]
        )
        .with_columns(duration=pl.col("end").sub(pl.col("start")))
        .with_columns(words=pl.col("text").str.split(" ").list.len())
        .with_columns(
            words_per_second=pl.col("words")
            / pl.col("duration").dt.milliseconds()
            * 1000
        )
        .with_columns(
            words_per_second=pl.when(pl.col("words_per_second").is_infinite())
            .then(pl.col("words").cast(pl.Float32))
            .otherwise(pl.col("words_per_second"))
        )
        .with_columns(
            group=pl.col("words_per_second").lt(MIN_WORDS_PER_SECOND).cum_sum()
        )
        .groupby("group")
        .agg(
            pl.col("path").first().alias("path"),
            pl.col("index").first().alias("index"),
            pl.col("start").min().alias("start"),
            pl.col("end").max().alias("end"),
            pl.col("text").str.concat(" ").alias("text"),
            pl.col("words").sum().alias("words"),
        )
        .sort("start")
        .with_columns(
            group2=pl.col("words").gt(MIN_WORDS_PER_CHUNK).cum_sum().alias("group2")
        )
        .groupby("group2")
        .agg(
            pl.col("path").first().alias("path"),
            pl.col("index").first().alias("index"),
            pl.col("start").min().alias("start"),
            pl.col("end").max().alias("end"),
            pl.col("text").str.concat(" ").alias("text"),
            pl.col("words").sum().alias("words"),
        )
    )
    return df_clean


def format_df_for_txt(df: pl.DataFrame) -> tuple[str, Path]:
    df_vals = df.sort("start").to_dict()
    output_path = Path(
        str(df_vals["path"][0]).replace(".srt", ".txt").replace("/srt/", "/txt/")
    )

    output = []
    for i in range(len(df)):
        start_str = str(df_vals["start"][i]).replace("2024-01-01 ", "")
        end_str = str(df_vals["end"][i]).replace("2024-01-01 ", "")
        output.append(f"{start_str} --> {end_str}")
        output.append(df_vals["text"][i] + "\n" * 3)

    output_str = "\n".join(output)
    return output_str, output_path


def write_srt_txt(txt: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        f.write(txt)


def get_srt_files() -> list[Path]:
    return list(Path(__file__).parent.glob("**/*.srt"))


def main():
    srt_files = get_srt_files()
    for srt_file in srt_files:
        parsed_srt = parse_srt(srt_file)
        cleaned_srt = clean_srt(parsed_srt)
        print(cleaned_srt)
        print(cleaned_srt.describe())

        srt_txt, txt_path = format_df_for_txt(cleaned_srt)
        write_srt_txt(srt_txt, txt_path)


if __name__ == "__main__":
    main()
