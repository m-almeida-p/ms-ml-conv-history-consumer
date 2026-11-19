#!/usr/bin/env python3
"""Checks that the documentation and the Designer metadata agree. Standard library only.

    python3 -I scripts/check-docs.py [--no-mermaid]

  1. every `Docs: <file> > <Section>` in a note of a Karavan project points to a file in docs/ and to a heading in it;
  2. every Markdown link in README.md, docs/ and the PROJECT.md files resolves: the file exists and so does the #anchor;
  3. every Designer label (a route or step `description`, numbered or not) is at most 32 characters: the Designer draws 32 and
     then `...`, measured by opening every route;
  4. every mermaid diagram renders (needs mmdc; skipped with --no-mermaid or when it is not installed).
"""
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECTS = ["conv-history-consumer"]
MAX_LABEL = 32
failures = []


def fail(msg):
    failures.append(msg)
    print(f"  FAIL  {msg}")


def slug(heading):
    """The anchor GitHub gives a heading: lower case, no punctuation but hyphens, spaces as hyphens."""
    text = re.sub(r"[`*_]", "", heading).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def headings(path):
    out = []
    in_code = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("```"):
            in_code = not in_code
        m = re.match(r"^#{1,6}\s+(.*?)\s*$", line)
        if m and not in_code:
            out.append(m.group(1))
    return out


def heading_matches(section, found):
    s = re.sub(r"[`*]", "", section).strip().lower()
    return any(re.sub(r"[`*]", "", h).strip().lower().startswith(s) for h in found)


# 1. Docs: pointers in the notes ------------------------------------------------------------------------------------------
print("== Docs: pointers in the Designer notes")
checked = 0
for project in PROJECTS:
    for yaml_file in sorted((ROOT / project).glob("*.camel.yaml")):
        for number, line in enumerate(yaml_file.read_text(encoding="utf-8").splitlines(), 1):
            for m in re.finditer(r"Docs: ([\w.-]+\.md) > ([^.\"]+(?:\.[^ \"]+)?)\.?", line):
                checked += 1
                doc, section = m.group(1), m.group(2).strip().rstrip(".")
                path = ROOT / "docs" / doc
                if not path.exists():
                    fail(f"{yaml_file.name}:{number}: docs/{doc} does not exist")
                elif not heading_matches(section, headings(path)):
                    fail(f"{yaml_file.name}:{number}: docs/{doc} has no heading '{section}'")
print(f"   {checked} pointers checked")

# 2. Markdown links -----------------------------------------------------------------------------------------------------------
print("== Markdown links")
files = [ROOT / "README.md"] + sorted((ROOT / "docs").glob("*.md")) + [ROOT / p / "PROJECT.md" for p in PROJECTS]
links = 0
for md in files:
    text = md.read_text(encoding="utf-8")
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    for m in re.finditer(r"\[[^\]]+\]\(([^)\s]+)\)", text):
        target = m.group(1)
        if re.match(r"^(https?:|mailto:)", target):
            continue
        links += 1
        file_part, _, anchor = target.partition("#")
        dest = (md.parent / file_part).resolve() if file_part else md
        if not dest.exists():
            fail(f"{md.relative_to(ROOT)}: link to {target}: the file does not exist")
        elif anchor and dest.suffix == ".md":
            if anchor not in [slug(h) for h in headings(dest)]:
                fail(f"{md.relative_to(ROOT)}: link to {target}: no heading with that anchor")
print(f"   {links} links checked")

# 3. Designer labels ----------------------------------------------------------------------------------------------------------
print(f"== Designer labels, at most {MAX_LABEL} characters")
labels = 0
for project in PROJECTS:
    for yaml_file in sorted((ROOT / project).glob("*.camel.yaml")):
        for number, line in enumerate(yaml_file.read_text(encoding="utf-8").splitlines(), 1):
            m = re.match(r"^\s*(?:- )?description: (.+?)\s*$", line)
            if m:
                labels += 1
                if len(m.group(1)) > MAX_LABEL:
                    fail(f"{yaml_file.name}:{number}: '{m.group(1)}' is {len(m.group(1))} characters")
print(f"   {labels} labels checked")

# 4. Mermaid ------------------------------------------------------------------------------------------------------------------
if "--no-mermaid" not in sys.argv:
    mmdc = shutil.which("mmdc") or str(Path.home() / ".local/bin/mmdc")
    print("== Mermaid diagrams render")
    if not Path(mmdc).exists():
        print("   mmdc is not installed: skipped")
    else:
        count = 0
        with tempfile.TemporaryDirectory() as tmp:
            for md in files:
                for i, block in enumerate(re.findall(r"```mermaid\n(.*?)```", md.read_text(encoding="utf-8"), re.S), 1):
                    count += 1
                    src = Path(tmp) / "d.mmd"
                    src.write_text(block, encoding="utf-8")
                    r = subprocess.run([mmdc, "-q", "-i", str(src), "-o", str(Path(tmp) / "d.svg")], capture_output=True, text=True)
                    if r.returncode != 0:
                        fail(f"{md.relative_to(ROOT)}: diagram {i} does not render: {r.stderr.strip().splitlines()[-1] if r.stderr.strip() else ''}")
        print(f"   {count} diagrams rendered")

print()
if failures:
    print(f"❌  {len(failures)} problem(s)")
    sys.exit(1)
print("✅  the docs and the Designer metadata agree")
