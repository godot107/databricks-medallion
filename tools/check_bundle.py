"""Offline structure check of the bundle YAML against the CLI's JSON schema.

`databricks bundle validate` needs a workspace login; this catches typos and
wrong nesting without one, so it runs on every PR (and before you push).

    databricks bundle schema > /tmp/bundle_schema.json
    python tools/check_bundle.py /tmp/bundle_schema.json
"""
import glob
import json
import sys

import jsonschema
import yaml


def load_bundle() -> dict:
    bundle = yaml.safe_load(open("databricks.yml"))
    for path in sorted(glob.glob("resources/*.yml")):
        for kind, items in yaml.safe_load(open(path))["resources"].items():
            bundle.setdefault("resources", {}).setdefault(kind, {}).update(items)
    return bundle


def main(schema_path: str) -> int:
    # The schema uses Go regex classes Python's re lacks; ASCII stand-ins are fine here.
    raw = open(schema_path).read().replace("\\\\p{L}", "a-zA-Z").replace("\\\\p{N}", "0-9")
    validator = jsonschema.Draft202012Validator(json.loads(raw))
    errors = list(validator.iter_errors(load_bundle()))
    for e in errors:
        print("/".join(map(str, e.absolute_path)), "->", e.message[:200])
    print(f"bundle schema check: {len(errors)} error(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
