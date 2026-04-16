"""Perceptual hashing (dHash) for tolerance-based snapshot matching.

Unlike cryptographic content hashes (which avalanche on any bit flip),
dHash produces similar hashes for visually similar images. Similarity is
measured by Hamming distance between two 64-bit hashes.

dHash works by comparing adjacent pixel gradients on a downsampled 9x8
grayscale image: bit N is set when pixel[i+1] > pixel[i]. Robust to
brightness, gamma, and minor rendering differences; cheap to compute
(single Pillow downsample + numpy compare).
"""

from __future__ import annotations

from io import BytesIO

import numpy as np
from PIL import Image

HAMMING_TOLERANCE_BITS = 6
"""Max Hamming distance (of 64) to treat two dHashes as the same image.

Community default for dHash on screenshots. Tune against the run history
before raising — above ~10 bits, false positives dominate.
"""


def compute_phash(content: bytes) -> str:
    """Return a 16-char hex dHash (64 bits) for the given image bytes.

    Deterministic: same input always produces the same hash regardless of
    platform, so hashes computed here are comparable to hashes stored by
    earlier runs.
    """
    with Image.open(BytesIO(content)) as img:
        small = img.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
        pixels = np.asarray(small, dtype=np.int16)

    diff = pixels[:, 1:] > pixels[:, :-1]
    bits = diff.flatten()
    value = 0
    for bit in bits:
        value = (value << 1) | int(bit)
    return f"{value:016x}"


def hamming_distance(a: str, b: str) -> int:
    """Bit difference between two 16-char hex dHashes.

    Returns 64 (max) if either input is empty or malformed — callers should
    treat that as "do not match."
    """
    if not a or not b or len(a) != len(b):
        return 64
    try:
        return bin(int(a, 16) ^ int(b, 16)).count("1")
    except ValueError:
        return 64


def is_within_tolerance(a: str, b: str, *, threshold: int = HAMMING_TOLERANCE_BITS) -> bool:
    return hamming_distance(a, b) <= threshold
