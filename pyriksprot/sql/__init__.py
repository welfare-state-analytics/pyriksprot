from pathlib import Path

SQL_ROOT = Path(__file__).resolve().parent


def sql_file_paths() -> list[Path]:
    return sorted([x for x in SQL_ROOT.iterdir() if x.suffix == '.sql'])


def sql_folder() -> Path:
    return SQL_ROOT


def metadata_version() -> str:
    try:
        return (sql_folder() / "version").read_text().strip()
    except FileNotFoundError:
        return "unknown"

def assert_metadata_version(expected_version: str):
    actual_version: str = metadata_version()
    if actual_version != expected_version:
        raise ValueError(f"Metadata version mismatch: expected {expected_version}, got {actual_version}")
