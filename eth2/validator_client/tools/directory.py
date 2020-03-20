from pathlib import Path


def create_dir_if_missing(path: Path) -> bool:
    if not path.exists():
        path.mkdir(parents=True)
        return True
    return False
