from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


# Data models
@dataclass(frozen=True)
class Player:
    """A player waiting in queue."""
    id: str
    role: str   # e.g., "heavy" | "medium" | "light" | "arty"
    tier: int   # e.g., 1..10
    mmr: float
    enqueued_at: float


@dataclass
class Match:
    """A formed match with two teams and metadata."""
    team_a: List[Player]
    team_b: List[Player]
    template_name: str
    center_tier: int


@dataclass
class RoleCaps:
    """Per-team minimum and maximum counts for each role, plus team size."""
    min_per_role: Dict[str, int]
    max_per_role: Dict[str, int]
    team_size: int


# Helpers
def age_priority(now: float, enqueued_at: float, alpha: float = 0.01) -> float:
    """Simple aging function: priority increases linearly with wait seconds."""
    return alpha * (now - enqueued_at if now > enqueued_at else 0.0)


def choose_center_tier(players: List[Player]) -> int:
    """Pick a center tier near the most common tier (ties favor mid-tier)."""
    if not players:
        return 6
    counts: Dict[int, int] = {}
    for p in players:
        counts[p.tier] = counts.get(p.tier, 0) + 1
    return max(counts.items(), key=lambda kv: (kv[1], -abs(kv[0] - 6)))[0]


def filter_eligible_by_tier(waiting: List[Player], center_tier: int, delta: int) -> List[Player]:
    """Return players within the allowed [center-delta, center+delta] tier window."""
    return [p for p in waiting if abs(p.tier - center_tier) <= delta]


def fill_team_by_caps(buckets: Dict[str, List[Player]], role_caps: RoleCaps, now: float) -> Optional[List[Player]]:
    """Greedy fill: satisfy per-role minimums first, then fill up to max via round-robin.
    Players within each role are chosen by highest aging priority (then MMR).
    Returns a full team or None if minimums cannot be met.
    """
    team: List[Player] = []
    used_ids: set[str] = set()

    def pick_best_from_role(role: str) -> Optional[Player]:
        pool = [p for p in buckets.get(role, []) if p.id not in used_ids]
        if not pool:
            return None
        pool.sort(key=lambda p: (age_priority(now, p.enqueued_at), p.mmr), reverse=True)
        best = pool[0]
        used_ids.add(best.id)
        return best

    # Satisfy minimums
    for role, minimum in role_caps.min_per_role.items():
        for _ in range(minimum):
            choice = pick_best_from_role(role)
            if not choice:
                return None
            team.append(choice)

    # Fill remaining slots up to per-role maximums (round-robin across roles)
    remaining_slots = role_caps.team_size - len(team)
    roles = list(role_caps.max_per_role.keys())
    counts = {r: sum(1 for p in team if p.role == r) for r in roles}
    idx = 0
    while remaining_slots > 0 and roles:
        role = roles[idx % len(roles)]
        current = counts.get(role, 0)
        limit = role_caps.max_per_role.get(role, role_caps.team_size)
        if current < limit:
            choice = pick_best_from_role(role)
            if choice:
                team.append(choice)
                counts[role] = current + 1
                remaining_slots -= 1
            else:
                roles.remove(role)
                if roles:
                    idx -= 1  # stay on the same index after removal
        idx += 1

    return team if len(team) == role_caps.team_size else None


# Core API
def build_match(
    waiting: List[Player],
    role_caps: RoleCaps,
    now: float,
    tier_delta_initial: int = 1,
    relax_after_secs: float = 60.0,
    tier_delta_relaxed: int = 2,
) -> Optional[Match]:
    """Build one match (two teams) if enough eligible players are present.
    Steps: choose center tier → filter by tier window (with relaxation) → fill two teams.
    """
    if len(waiting) < role_caps.team_size * 2:
        return None

    center = choose_center_tier(waiting)
    oldest_enqueued = min(p.enqueued_at for p in waiting)
    waited_long_enough = (now - oldest_enqueued) >= relax_after_secs
    delta = tier_delta_relaxed if waited_long_enough else tier_delta_initial

    eligible = filter_eligible_by_tier(waiting, center, delta)
    if len(eligible) < role_caps.team_size * 2:
        return None

    # Build role buckets from eligible players
    buckets: Dict[str, List[Player]] = {}
    for p in eligible:
        buckets.setdefault(p.role, []).append(p)

    team1 = fill_team_by_caps(buckets, role_caps, now)
    if not team1:
        return None

    chosen_ids = {p.id for p in team1}
    buckets_remaining = {r: [p for p in lst if p.id not in chosen_ids] for r, lst in buckets.items()}
    team2 = fill_team_by_caps(buckets_remaining, role_caps, now)
    if not team2:
        return None

    return Match(team_a=team1, team_b=team2, template_name="balanced", center_tier=center)


def match_tick(
    waiting: List[Player],
    role_caps: RoleCaps,
    now: float,
    limit: int = 1,
) -> Tuple[List[Match], List[Player]]:
    """Form up to `limit` matches from the waiting list and return (matches, remaining)."""
    remaining = list(waiting)
    matches: List[Match] = []
    formed = 0
    while formed < limit:
        result = build_match(remaining, role_caps, now)
        if not result:
            break
        matches.append(result)
        formed += 1
        used_ids = {p.id for p in result.team_a + result.team_b}
        remaining = [p for p in remaining if p.id not in used_ids]
    return matches, remaining


DEFAULT_ROLE_CAPS = RoleCaps(
    min_per_role={"heavy": 2, "medium": 4, "light": 2, "arty": 0},
    max_per_role={"heavy": 5, "medium": 8, "light": 4, "arty": 2},
    team_size=15,
)
