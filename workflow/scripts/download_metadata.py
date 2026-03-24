from pathlib import Path
from urllib.request import urlopen


METADATA_URLS = {
    "well_metadata": "well.csv.gz",
    "plate_metadata": "plate.csv.gz",
    "compound_metadata": "compound.csv.gz",
    "control_metadata": "perturbation_control.csv",
    "compound_source_metadata": "compound_source.csv.gz",
}


def parse_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def download_file(url, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".tmp")

    with urlopen(url) as response, open(tmp_path, "wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)

    tmp_path.replace(destination)


base_url = snakemake.config["metadata"]["base_url"].rstrip("/")
overwrite = parse_bool(snakemake.config["download"].get("overwrite", False))

for output_name, output_path in snakemake.output.items():
    destination = Path(output_path)
    filename = METADATA_URLS[output_name]
    url = f"{base_url}/{filename}"

    if destination.exists() and destination.stat().st_size > 0 and not overwrite:
        print(f"[download_metadata] skip existing {destination}", flush=True)
        continue

    print(f"[download_metadata] {url} -> {destination}", flush=True)
    download_file(url=url, destination=destination)
