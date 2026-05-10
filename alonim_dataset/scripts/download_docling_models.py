from __future__ import annotations

import os
from pathlib import Path

from huggingface_hub import snapshot_download


MODELS = (
    {
        "repo_id": "docling-project/docling-layout-heron",
        "revision": "main",
        "folder": "docling-project--docling-layout-heron",
    },
    {
        "repo_id": "docling-project/docling-models",
        "revision": "v2.3.0",
        "folder": "docling-project--docling-models",
    },
)


def main() -> None:
    artifacts_root = Path(
        os.environ.get(
            "DOCLING_ARTIFACTS_PATH",
            Path.home() / ".cache" / "docling" / "artifacts",
        )
    ).expanduser()
    artifacts_root.mkdir(parents=True, exist_ok=True)

    print(f"Docling artifacts path: {artifacts_root}")
    print(f"HF_HUB_DISABLE_XET: {os.environ.get('HF_HUB_DISABLE_XET', '')}")

    for model in MODELS:
        target_dir = artifacts_root / model["folder"]
        target_dir.mkdir(parents=True, exist_ok=True)

        print()
        print(f"Downloading {model['repo_id']} @ {model['revision']}")
        print(f"Target: {target_dir}")
        path = snapshot_download(
            repo_id=model["repo_id"],
            revision=model["revision"],
            local_dir=target_dir,
        )
        print(f"Ready: {path}")

    print()
    print("Docling model downloads completed.")


if __name__ == "__main__":
    main()
