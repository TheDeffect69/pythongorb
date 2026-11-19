"""Serve an operator-owned local catalog through the laboratory source protocol."""

import hashlib
import json
from pathlib import Path


class MediaCatalog:
    def __init__(self, manifest):
        self.root = manifest.resolve().parent
        self.entries = json.loads(manifest.read_text(encoding="utf-8"))
        if not isinstance(self.entries, dict):
            raise ValueError("Expected an object keyed by numeric ID")
        for key, entry in self.entries.items():
            if not key.isascii() or not key.isdecimal() or str(int(key)) != key:
                raise ValueError("Invalid catalog ID")
            if not isinstance(entry, dict) or not isinstance(entry.get("title"), str):
                raise ValueError("Missing title")
            if type(entry.get("download_allowed")) is not bool:
                raise ValueError("Set download_allowed explicitly for each item")
            if entry["download_allowed"]:
                self.file_for(entry)

    def file_for(self, entry):
        relative = Path(entry["file"])
        path = (self.root / relative).resolve(strict=True)
        if relative.is_absolute() or not path.is_relative_to(self.root) or not path.is_file():
            raise ValueError("File must be inside the manifest directory")
        return path

    def serve(self, handler, route, item_id):
        entry = self.entries.get(str(item_id))
        if entry is None:
            handler.send_json(404, {})
            return
        record = {"id": item_id, "title": entry["title"], "download_allowed": entry["download_allowed"],
                  "source_url": f"https://kinescope.io/{item_id}"}
        if not entry["download_allowed"]:
            handler.send_json(200 if route == "media" else 403, record if route == "media" else {})
            return
        try:
            file = self.file_for(entry).open("rb")
        except (OSError, ValueError):
            handler.send_json(503, {"error": "catalog_file_unavailable"})
            return
        with file:
            size = file.seek(0, 2)
            file.seek(0)
            mime = entry.get("mime_type", "video/mp4")
            if mime not in {"video/mp4", "video/webm", "video/quicktime", "application/octet-stream"}:
                mime = "application/octet-stream"
            if route == "media":
                handler.send_json(200, dict(record, size=size, sha256=hashlib.file_digest(file, "sha256").hexdigest(),
                                            mime_type=mime, download_url=f"/files/{item_id}"))
                return
            offset = handler.start_file(size, mime, allow_range=True)
            if offset is None:
                return
            file.seek(offset)
            while block := file.read(65536):
                handler.wfile.write(block)
