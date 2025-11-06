from __future__ import annotations

import re
import sys
from pathlib import Path

PATTERNS = {
    "AWS Access Key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "AWS Secret Key": re.compile(r"(?i)aws_secret_access_key\s*=\s*['\"]?[0-9A-Za-z/+]{40}"),
    "Slack Token": re.compile(r"xox[aboprs]-[0-9A-Za-z-]{10,}"),
    "Google API Key": re.compile(r"AIza[0-9A-Za-z_-]{35}"),
}

SAFE_PREFIXES = ("YOUR_", "REPLACE_ME", "DUMMY")

SEARCH_ROOTS = (Path("src"), Path("tests"), Path("config"))


def main() -> int:
    findings: list[tuple[str, str, str]] = []
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for label, pattern in PATTERNS.items():
                for match in pattern.finditer(text):
                    candidate = match.group(0)
                    if candidate.startswith(SAFE_PREFIXES):
                        continue
                    findings.append((label, str(path), candidate))
    if findings:
        print("Potential secrets detected:")
        for label, path, snippet in findings:
            print(f" - {label} in {path}: {snippet[:12]}...")
        return 1
    print("No secrets detected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
