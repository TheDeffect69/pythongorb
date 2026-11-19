"""Build the numeric range index from videos visible to a Kinescope workspace token."""

import argparse
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlencode

from agent import Kinescope, SourceError


def build_index(source, project_id):
    index = {}
    skipped = 0
    page = 1
    while True:
        query = urlencode(dict(page=page, per_page=100, project_id=project_id, order="created_at.asc"))
        reply = source.api_json("?" + query, 30)
        records = reply["data"]
        pagination = reply["meta"]["pagination"]
        if not isinstance(records, list) or int(pagination["page"]) != page:
            raise ValueError("Unexpected pagination in Kinescope response")
        for video in records:
            if video.get("project_id") != project_id:
                raise ValueError("Video outside requested project")
            match = re.fullmatch(r"https://kinescope\.io/(0|[1-9][0-9]{0,18})/?", video.get("play_link", ""))
            if match:
                number = match.group(1)
                if number in index and index[number] != video["id"]:
                    raise ValueError("Two videos have the same numeric link")
                index[number] = video["id"]
            else:
                skipped += 1
        if not records or page * int(pagination["per_page"]) >= int(pagination["total"]):
            return index, skipped
        page += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        index, skipped = build_index(Kinescope({}, os.getenv("LAB_KINESCOPE_TOKEN")), args.project)
    except (ValueError, SourceError, KeyError) as exc:
        parser.exit(1, f"Workspace index unavailable: {exc}\n")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=args.output.parent, encoding="utf-8", delete=False) as temp:
        json.dump(index, temp, indent=2)
    os.replace(temp.name, args.output)
    print(f"Numeric player links: {len(index)}; videos without numeric links: {skipped}")
