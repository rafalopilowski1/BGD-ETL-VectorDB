#!/usr/bin/env python3

from pathlib import Path

from tqdm import tqdm


def count_lines(path: Path) -> int:
    count = 0
    with path.open("rb") as f:
        for _ in tqdm(f, desc="Counting lines", unit=" lines"):
            count += 1
    return count


def split_jsonl(input_path: Path, output_dir: Path, n_parts: int = 3) -> None:
    """Split JSONL into n_parts without loading the file into memory."""
    total = count_lines(input_path)
    rows_per_part = total // n_parts
    print(f"Total: {total} rows, ~{rows_per_part} rows per part")

    part = 0
    written = 0
    output_path = output_dir / f"part_{part + 1:02d}.jsonl"
    out_file = output_path.open("w")

    with input_path.open("r") as f:
        for line in tqdm(f, total=total, desc="Splitting", unit=" lines"):
            if part < n_parts - 1 and written == rows_per_part:
                out_file.close()
                tqdm.write(f"Written {output_path}: {written} rows")
                part += 1
                written = 0
                output_path = output_dir / f"part_{part + 1:02d}.jsonl"
                out_file = output_path.open("w")

            out_file.write(line)
            written += 1

    out_file.close()
    print(f"Written {output_path}: {written} rows")
    print(f"Done: {total} rows split into {n_parts} files")


if __name__ == "__main__":
    split_jsonl(Path("data/arxiv-abstracts.jsonl"), Path("data/split"), n_parts=3)
