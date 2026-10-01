"""Check release archives for valid checksums and complete public documentation."""
import hashlib
from pathlib import Path, PurePosixPath
import posixpath
import re
import sys
import tarfile
from urllib.parse import unquote, urlsplit
import zipfile


def verify(directory):
    directory = Path(directory)
    entries = [line.split(maxsplit=1) for line in (directory / "SHA256SUMS").read_text().splitlines()]
    if len(entries) != 5 or len({name for _, name in entries}) != 5:
        raise ValueError("expected checksums for five distinct release archives")
    for digest, name in entries:
        archive = directory / name
        if hashlib.sha256(archive.read_bytes()).hexdigest() != digest:
            raise ValueError(f"{name}: checksum mismatch")
        if name.endswith(".zip"):
            with zipfile.ZipFile(archive) as source:
                files = {info.filename: source.read(info) for info in source.infolist() if not info.is_dir()}
        else:
            with tarfile.open(archive) as source:
                files = {info.name: source.extractfile(info).read() for info in source if info.isfile()}
        roots = {PurePosixPath(path).parts[0] for path in files}
        if len(roots) != 1:
            raise ValueError(f"{name}: expected one archive root")
        root = roots.pop()
        for required in ("README.md", "CHANGELOG.md", "docs/releases/v0.1.0.md", "docs/releases/v0.2.0.md", "docs/releases/v0.3.0.md"):
            if f"{root}/{required}" not in files:
                raise ValueError(f"{name}: missing {required}")
        for path, data in files.items():
            if not path.endswith(".md"):
                continue
            for link in re.findall(r"\]\(([^\s)]+)\)", data.decode()):
                url = urlsplit(link)
                if url.scheme or url.netloc:
                    continue
                target = posixpath.normpath(posixpath.join(posixpath.dirname(path), unquote(url.path))) if url.path else path
                if target not in files:
                    raise ValueError(f"{name}: {path} links to missing {link}")
                if url.fragment and target.endswith(".md"):
                    headings = re.findall(r"^#+\s+(.+)$", files[target].decode(), re.MULTILINE)
                    anchors = {re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-") for heading in headings}
                    if url.fragment not in anchors:
                        raise ValueError(f"{name}: {path} links to missing anchor {link}")
        print(f"{name}: checksums and documentation OK")


if __name__ == "__main__":
    verify(sys.argv[1] if len(sys.argv) > 1 else "dist")
