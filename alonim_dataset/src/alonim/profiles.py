from __future__ import annotations

from enum import StrEnum
from pathlib import Path


class BulletinProfile(StrEnum):
    DEFAULT = "default"
    MORDECHAI_BLASS = "mordechai_blass"
    METIKUT_HAPARSHA = "metikut_haparsha"
    BIRKAT_YITZCHAK = "birkat_yitzchak"


def profile_for_source(source_path: Path | str | None) -> BulletinProfile:
    if source_path is None:
        return BulletinProfile.DEFAULT

    source = str(source_path)
    if "מאמרי הרב מרדכי בלס" in source:
        return BulletinProfile.MORDECHAI_BLASS
    if "מתיקות הפרשה" in source or "הרב אריה לוין" in source:
        return BulletinProfile.METIKUT_HAPARSHA
    if "שיעורי ליל שישי" in source or "ברכת יצחק" in source:
        return BulletinProfile.BIRKAT_YITZCHAK
    return BulletinProfile.DEFAULT
