import argparse
import csv
import json
from pathlib import Path
from urllib.request import Request, urlopen


DEFAULT_INPUT = Path("data/procdata/metadata/drug_metadata_raw.tsv")
DEFAULT_OUTPUT = Path("data/procdata/metadata/missing_annotationdb.txt")
DEFAULT_ANNOTATIONDB_API = "https://v2annotationdb.bhklab.ca"
INCHIKEY_COLUMN = "Metadata_InChIKey"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Compare JUMP compound metadata against AnnotationDB and write the "
            "missing InChIKeys to a line-delimited text file."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input TSV with a {INCHIKEY_COLUMN} column.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output text file with one missing InChIKey per line.",
    )
    parser.add_argument(
        "--annotationdb-api",
        default=DEFAULT_ANNOTATIONDB_API,
        help="AnnotationDB API base URL.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Timeout in seconds for the AnnotationDB request.",
    )
    return parser.parse_args()


def read_jump_rows(path):
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames is None or INCHIKEY_COLUMN not in reader.fieldnames:
            raise ValueError(f"Input file must contain a {INCHIKEY_COLUMN} column")

        rows = list(reader)

    return rows


def fetch_annotationdb_inchikeys(url, timeout):
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "cellpainting-annotationdb-missing-compounds/1.0",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        payload = json.load(response)

    return {
        record["inchikey"].strip()
        for record in payload
        if isinstance(record, dict) and record.get("inchikey")
    }


def missing_inchikeys(rows, available_inchikeys):
    filtered_inchikeys = []
    seen = set()

    for row in rows:
        inchikey = (row.get(INCHIKEY_COLUMN) or "").strip()
        if not inchikey:
            continue
        if inchikey in available_inchikeys:
            continue
        if inchikey in seen:
            continue

        filtered_inchikeys.append(inchikey)
        seen.add(inchikey)

    return filtered_inchikeys


def write_inchikeys(path, inchikeys):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for inchikey in inchikeys:
            handle.write(f"{inchikey}\n")


def main():
    args = parse_args()

    rows = read_jump_rows(args.input)
    annotationdb_endpoint = f"{args.annotationdb_api.rstrip('/')}/compound/all"
    available_inchikeys = fetch_annotationdb_inchikeys(
        url=annotationdb_endpoint,
        timeout=args.timeout,
    )
    inchikeys_missing_from_annotationdb = missing_inchikeys(
        rows=rows,
        available_inchikeys=available_inchikeys,
    )
    write_inchikeys(args.output, inchikeys_missing_from_annotationdb)

    print(
        "[find_annotationdb_missing_compounds] "
        f"input_rows={len(rows)} "
        f"annotationdb_inchikeys={len(available_inchikeys)} "
        f"missing_inchikeys={len(inchikeys_missing_from_annotationdb)} "
        f"output={args.output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
