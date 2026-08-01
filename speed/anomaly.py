from __future__ import annotations


def disturbance_score(speed: float, density: float, bt: float) -> int:
    score = 0
    score += 5 if speed >= 700 else 4 if speed >= 600 else 3 if speed >= 500 else 1
    score += 5 if density >= 10 else 4 if density >= 9 else 3 if density >= 8 else 1
    score += 5 if bt >= 8 else 4 if bt >= 7 else 3 if bt >= 6 else 1
    return score


def classify(score: int) -> str:
    if score >= 13:
        return "extreme"
    if score >= 10:
        return "high"
    if score >= 7:
        return "elevated"
    return "baseline"
