import subprocess
from pathlib import Path


def parse_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


destination = Path(snakemake.output[0])
destination.parent.mkdir(parents=True, exist_ok=True)

s3_uri = snakemake.params.s3_uri
expected_size = int(snakemake.params.expected_size)
overwrite = parse_bool(snakemake.params.overwrite)

if (
    destination.exists()
    and destination.stat().st_size == expected_size
    and not overwrite
):
    print(f"[download_profile] skip existing {destination}", flush=True)
else:
    if destination.exists():
        actual_size = destination.stat().st_size
        print(
            f"[download_profile] warning replacing existing file {destination} "
            f"(actual_size={actual_size}, expected_size={expected_size})",
            flush=True,
        )

    cmd = [
        "aws",
        "s3",
        "cp",
        "--no-sign-request",
        s3_uri,
        str(destination),
    ]
    print(
        f"[download_profile] download {s3_uri} -> {destination} "
        f"(expected_size={expected_size})",
        flush=True,
    )
    subprocess.run(cmd, check=True)

actual_size = destination.stat().st_size
if actual_size != expected_size:
    raise ValueError(
        f"Downloaded size mismatch for {destination}: "
        f"expected {expected_size}, got {actual_size}"
    )
