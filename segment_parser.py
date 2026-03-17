"""
Segment Parser — AI-Powered Insurance Data Engineering Agent
Parses "Agency/PB Seg" strings from HM_DIGIT grid files into standardized JSON.

Handles:
  - Delimiter normalization (MC_=180, MC <= 180, MC-180 → canonical form)
  - Vehicle category extraction (MC→Bike, SC→Scooter)
  - CC range parsing with mathematical aliases
  - Fuel type detection (EV/Electric → Electric, default Petrol)
  - SC/EV explosion (duplication into Petrol + Electric paths)
  - Manufacturer inclusion/exclusion logic
  - Confidence scoring with HITL queue routing
"""

import re
import json
import os
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from enum import Enum

from pydantic import BaseModel


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class VehicleCategory(str, Enum):
    BIKE = "Bike"
    SCOOTER = "Scooter"
    ALL = "All"

class FuelType(str, Enum):
    PETROL = "Petrol"
    ELECTRIC = "Electric"
    ALL = "All"


@dataclass
class ParsedSegment:
    """Structured output from segment parsing."""
    category: str                          # Bike / Scooter / All
    cc_range: Optional[Dict[str, int]]     # {"from": 1, "to": 180} or None
    fuel: str                              # Petrol / Electric / All
    manufacturers: Dict[str, List[str]]    # {"include": [...], "exclude": [...]}
    duplication_required: bool             # True if SC/EV split needed
    confidence_score: float                # 0.0–1.0
    canonical_form: str                    # Normalized string
    original_input: str                    # Raw input string
    warnings: List[str] = field(default_factory=list)  # Any parsing warnings

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, indent=2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    # Pipeline-compatible output formats
    def to_portal_vehicle_type(self) -> str:
        return {"Bike": "BIKE", "Scooter": "SCOOTER", "All": "ALL"}[self.category]

    def to_portal_fuel(self) -> str:
        return {"Petrol": "PETROL", "Electric": "ELETRIC", "All": "ALL"}[self.fuel]

    def to_portal_make(self) -> str:
        if self.manufacturers["exclude"]:
            return "EXCLUDE: " + ", ".join(self.manufacturers["exclude"])
        elif self.manufacturers["include"]:
            return ", ".join(self.manufacturers["include"])
        return "ALL"

    def to_portal_cc_from(self) -> Optional[str]:
        return str(self.cc_range["from"]) if self.cc_range else None

    def to_portal_cc_to(self) -> Optional[str]:
        return str(self.cc_range["to"]) if self.cc_range else None


# ═══════════════════════════════════════════════════════════════════════════════
# DELIMITER NORMALIZATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

# Known manufacturer name canonicalization
MAKE_CANONICAL = {
    "HERO": "HERO MOTOCORP",
    "HERO MOTOCORP": "HERO MOTOCORP",
    "HONDA": "HONDA",
    "Honda": "HONDA",
    "BAJAJ": "BAJAJ",
    "ROYAL ENFIELD": "ROYAL ENFIELD",
    "RE": "ROYAL ENFIELD",
    "SUZUKI": "SUZUKI",
    "TVS": "TVS",
    "YAMAHA": "YAMAHA",
    "JAWA": "JAWA MOTORCYCLE",
    "JAWA MOTORCYCLE": "JAWA MOTORCYCLE",
    "Avenger": "BAJAJ",  # Avenger is a Bajaj model
}


def normalize_delimiter(raw: str) -> str:
    """
    Strip noise from segment strings into canonical form.
    MC_=180, MC <= 180, MC-180 → MC_LE_180
    MC >155, MC>155 → MC_GT_155
    """
    s = raw.strip()

    # Normalize whitespace
    s = re.sub(r'\s+', ' ', s)

    # Normalize underscore-equals to <=
    s = s.replace('_=', ' <=')

    # Normalize various delimiter styles
    s = re.sub(r'\s*<=\s*', '_LE_', s)
    s = re.sub(r'\s*>=\s*', '_GE_', s)
    s = re.sub(r'\s*<\s*', '_LT_', s)
    s = re.sub(r'\s*>\s*', '_GT_', s)

    return s


# ═══════════════════════════════════════════════════════════════════════════════
# CC RANGE RESOLVER
# ═══════════════════════════════════════════════════════════════════════════════

# CC boundary lookup — numerical awareness for motor insurance context
CC_BOUNDARIES = {
    "155": {"le": (1, 155), "lt": (1, 155), "gt": (156, 2500), "ge": (156, 2500)},
    "180": {"le": (1, 180), "lt": (1, 180), "gt": (181, 2500), "ge": (181, 2500)},
    "350": {"le": (1, 350), "lt": (1, 350), "gt": (351, 2500), "ge": (351, 2500)},
}


def resolve_cc_range(canonical: str) -> Optional[Dict[str, int]]:
    """
    Resolve CC range from canonical segment form.
    Returns {"from": int, "to": int} or None.
    """
    # Pattern: MC_LE_180, MC_GT_155, MC_LT_155
    m = re.search(r'(?:MC|BIKE)_?(LE|LT|GE|GT)_?(\d+)', canonical, re.IGNORECASE)
    if m:
        op = m.group(1).lower()
        num = m.group(2)
        if num in CC_BOUNDARIES:
            return dict(zip(["from", "to"], CC_BOUNDARIES[num].get(op, (None, None))))
        # Fallback arithmetic
        val = int(num)
        if op in ("le", "lt"):
            return {"from": 1, "to": val}
        elif op in ("ge", "gt"):
            return {"from": val + 1, "to": 2500}

    # Explicit range: 180-350
    m = re.search(r'(\d+)\s*[-–]\s*(\d+)', canonical)
    if m:
        return {"from": int(m.group(1)), "to": int(m.group(2))}

    # Standalone <=350, >350 patterns
    m = re.search(r'_LE_(\d+)|<=\s*(\d+)', canonical)
    if m:
        val = int(m.group(1) or m.group(2))
        return {"from": 1, "to": val}

    m = re.search(r'_GT_(\d+)|>\s*(\d+)', canonical)
    if m:
        val = int(m.group(1) or m.group(2))
        return {"from": val + 1, "to": 2500}

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# MANUFACTURER PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_manufacturers(segment_str: str, make_column: Optional[str] = None,
                        all_cluster_makes: Optional[set] = None) -> Dict[str, List[str]]:
    """
    Extract manufacturer inclusion/exclusion from segment or make column.

    Rules:
    - If segment contains explicit names (Hero/Honda), → include
    - If segment says "Others" or "Other than X", → exclude X
    - If make_column is "Others" and we know all_cluster_makes, → exclude all explicit
    """
    include = []
    exclude = []

    # Check segment for embedded manufacturers
    # Pattern: "MC <= 180 Hero/Honda"
    mfr_match = re.search(r'\b(Hero|Honda|TVS|Bajaj|RE|JAWA|Avenger|ROYAL ENFIELD|SUZUKI|YAMAHA|HERO MOTOCORP)(?:/(\w+))*',
                          segment_str, re.IGNORECASE)

    if "Other than RE" in segment_str or "Other than" in segment_str:
        # Extract what's being excluded
        excl_match = re.search(r'Other than\s+(.+)', segment_str, re.IGNORECASE)
        if excl_match:
            names = re.split(r'[/,\s]+', excl_match.group(1))
            for n in names:
                n = n.strip()
                canonical = MAKE_CANONICAL.get(n, n.upper())
                if canonical:
                    exclude.append(canonical)

    elif "Others" in segment_str:
        # Generic "Others" in segment — will be handled by context
        pass

    elif mfr_match:
        # Extract all slash-separated names
        full = mfr_match.group(0)
        names = re.split(r'[/]', full)
        for n in names:
            n = n.strip()
            canonical = MAKE_CANONICAL.get(n, MAKE_CANONICAL.get(n.upper(), n.upper()))
            if canonical and canonical not in include and n.upper() != "AVENGER":
                include.append(canonical)

    # Handle make_column-level logic
    if make_column:
        make_str = make_column.strip()
        if make_str.upper() == "OTHERS":
            if all_cluster_makes:
                exclude = sorted(all_cluster_makes)
        elif "/" in make_str:
            # Combined makes like SUZUKI/TVS/YAMAHA
            for m in make_str.split("/"):
                m = m.strip()
                if m.upper() != "OTHERS":
                    canonical = MAKE_CANONICAL.get(m, MAKE_CANONICAL.get(m.upper(), m.upper()))
                    if canonical and canonical not in include:
                        include.append(canonical)
                else:
                    # e.g., "YAMAHA/Others" → YAMAHA is include, but "Others" needs exclude context
                    if all_cluster_makes:
                        # Include YAMAHA explicitly, exclude list comes from context
                        pass

    return {"include": include, "exclude": exclude}


# ═══════════════════════════════════════════════════════════════════════════════
# CORE PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_segment(raw_input: str,
                  make_column: Optional[str] = None,
                  all_cluster_makes: Optional[set] = None,
                  sheet_context: str = "1plus1") -> List[ParsedSegment]:
    """
    Parse a segment string into one or more ParsedSegment objects.

    Args:
        raw_input: The segment string (e.g., "MC <= 180 Hero/Honda")
        make_column: The Make column value (for make-specific sheets)
        all_cluster_makes: Set of all explicit makes in this cluster (for EXCLUDE logic)
        sheet_context: "1plus1" | "make_specific" | "saod"

    Returns:
        List of ParsedSegment (usually 1, but 2 for SC/EV splits)
    """
    original = raw_input.strip()
    canonical = normalize_delimiter(original)
    confidence = 1.0
    warnings = []
    results = []

    # ── SPECIAL: SC/EV or SCOOTER/EV — Duplication Required ──
    if original in ("SC/EV", "SCOOTER/EV", "SC_EV"):
        # Generate TWO records
        results.append(ParsedSegment(
            category="Scooter", cc_range=None, fuel="Petrol",
            manufacturers={"include": [], "exclude": []},
            duplication_required=True, confidence_score=0.99,
            canonical_form="SC_EV_PETROL", original_input=original,
        ))
        results.append(ParsedSegment(
            category="Scooter", cc_range=None, fuel="Electric",
            manufacturers={"include": [], "exclude": []},
            duplication_required=True, confidence_score=0.99,
            canonical_form="SC_EV_ELECTRIC", original_input=original,
        ))
        return results

    # ── DETECT VEHICLE CATEGORY ──
    category = VehicleCategory.ALL
    s_upper = original.upper()

    if s_upper.startswith("MC") or s_upper.startswith("BIKE"):
        category = VehicleCategory.BIKE
    elif s_upper.startswith("SC") or "SCOOTER" in s_upper or "MOPED" in s_upper:
        category = VehicleCategory.SCOOTER
    elif s_upper == "RE":
        category = VehicleCategory.BIKE  # Royal Enfield = Bike
    elif s_upper == "EV":
        category = VehicleCategory.ALL
    elif "KW" in s_upper:
        category = VehicleCategory.ALL  # EV power rating
    elif s_upper.startswith("SCOOTER/MC"):
        category = VehicleCategory.ALL
    elif s_upper in ("ALL", "EVERYTHING", "N/A", "TBD", "---", "") or "3W" in s_upper:
        category = VehicleCategory.ALL
    else:
        # Check if it looks like a CC range (starts with < or >)
        if re.match(r'^[<>]', original.strip()):
            category = VehicleCategory.BIKE
        else:
            warnings.append(f"Could not determine category from '{original}'")
            confidence -= 0.15

    # ── DETECT FUEL TYPE ──
    fuel = FuelType.PETROL
    if "EV" in s_upper or "ELECTRIC" in s_upper or "KW" in s_upper:
        fuel = FuelType.ELECTRIC

    # ── DETECT CC RANGE ──
    cc_range = None

    # MC <= 180, MC <=155, MC >155, MC >350
    cc_match = re.search(r'(?:MC|BIKE|3W|SC|SCOOTER)?\s*([<>]=?)\s*(\d+)', original, re.IGNORECASE)
    if cc_match:
        op = cc_match.group(1)
        val = int(cc_match.group(2))
        if op in ("<=", "<"):
            cc_range = {"from": 1, "to": val}
        elif op in (">", ">="):
            cc_range = {"from": val + 1, "to": 2500}

    # MC_180-350, 180-350
    range_match = re.search(r'(\d+)\s*[-–]\s*(\d+)', original)
    if range_match and not cc_range:
        cc_range = {"from": int(range_match.group(1)), "to": int(range_match.group(2))}

    # MC>350 (no space)
    if not cc_range:
        gt_match = re.search(r'MC\s*>(\d+)', original, re.IGNORECASE)
        if gt_match:
            val = int(gt_match.group(1))
            cc_range = {"from": val + 1, "to": 2500}

    # MC <155 (with space, no =)
    if not cc_range:
        lt_match = re.search(r'MC\s*<(\d+)', original, re.IGNORECASE)
        if lt_match:
            val = int(lt_match.group(1))
            cc_range = {"from": 1, "to": val}

    # Standalone < =350 or > 350 (from make-specific sheets)
    if not cc_range:
        le_match = re.match(r'<\s*=?\s*(\d+)', original)
        if le_match:
            cc_range = {"from": 1, "to": int(le_match.group(1))}
        gt_match = re.match(r'>\s*(\d+)', original)
        if gt_match:
            val = int(gt_match.group(1))
            cc_range = {"from": val + 1, "to": 2500}

    # KW ranges are NOT CC ranges — leave cc_range as None
    if "KW" in s_upper:
        cc_range = None

    # ── DETECT MANUFACTURERS ──
    manufacturers = parse_manufacturers(original, make_column, all_cluster_makes)

    # Special case: RE alone = Royal Enfield
    if original.strip() == "RE":
        manufacturers = {"include": ["ROYAL ENFIELD"], "exclude": []}
        confidence = 0.99

    # Special case: segment suffix _RE = Royal Enfield (e.g., MC_180-350_RE)
    if re.search(r'_RE$', original) and not manufacturers["include"]:
        manufacturers = {"include": ["ROYAL ENFIELD"], "exclude": []}

    # ── CALCULATE CONFIDENCE ──
    known_patterns = {
        "SC/EV", "SC", "SC_EV", "SCOOTER", "EV", "All", "MC", "RE",
        "SCOOTER/MC", "SCOOTER/EV", "ALL", "EVERYTHING", "N/A", "TBD", "---", "", "MOPED", "3W"
    }

    known_patterns_upper = {p.upper() for p in known_patterns}

    # Check if it matches a known pattern
    base = re.sub(r'\s*(Hero|Honda|TVS|Others|RE|JAWA|Avenger|Other than \w+).*', '', original).strip()

    if original.upper() in known_patterns_upper or base.upper() in known_patterns_upper:
        confidence = min(confidence, 0.99)
    elif cc_match or range_match:
        confidence = min(confidence, 0.98)
    elif "KW" in s_upper:
        confidence = min(confidence, 0.95)
    elif category == VehicleCategory.ALL and not cc_range and fuel == FuelType.PETROL:
        if original.upper() not in known_patterns_upper and base.upper() not in known_patterns_upper:
            confidence -= 0.10
            warnings.append("Ambiguous segment — defaulting to ALL/PETROL")

    # Check if confidence dropped to manual review levels
    if confidence < 0.95:
        # LLM Fallback Mechanism
        llm_results = _llm_fallback_parse(original, make_column, all_cluster_makes, sheet_context)
        if llm_results:
            return llm_results
        # If LLM failed, fall through to returning the low-confidence parsed segment

    results.append(ParsedSegment(
        category=category.value,
        cc_range=cc_range,
        fuel=fuel.value,
        manufacturers=manufacturers,
        duplication_required=False, # We don't try to guess duplication in fallback
        confidence_score=round(confidence, 2),
        canonical_form=canonical,
        original_input=original,
        warnings=warnings,
    ))
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# LLM FALLBACK PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class LLMParsedSegmentModel(BaseModel):
    category: str
    fuel: str
    include_makes: List[str]
    exclude_makes: List[str]
    duplication_required: bool
    cc_range_from: Optional[int]
    cc_range_to: Optional[int]

class LLMResponseModel(BaseModel):
    segments: List[LLMParsedSegmentModel]
    confidence_score: float

def _llm_fallback_parse(original: str, make_column: Optional[str],
                        all_cluster_makes: Optional[set], sheet_context: str) -> Optional[List[ParsedSegment]]:
    """Use an LLM to parse segment strings that the regex engine is unsure of."""
    model_name = os.environ.get("SEGMENT_PARSER_MODEL", "gpt-4o-mini") # default is fast/cheap
    
    # We only run if a key is present or using a local open model
    if not any(k in os.environ for k in ["OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "LITELLM_API_KEY", "AZURE_API_KEY"]):
        return None
    try:
        import litellm
    except Exception:
        return None

    sys_prompt = f"""You are an Insurance Data Engineering Agent.
Your goal is to parse the "Agency/PB Seg" column from a motor underwriting grid into standardized fields.
The original segment string is: "{original}"

Context:
- Make Column: {make_column or "N/A"}
- All Makes in Cluster: {all_cluster_makes or "N/A"}
- Sheet Context: {sheet_context}

Rules:
1. Category MUST be "Bike", "Scooter", or "All".
2. Fuel MUST be "Petrol", "Electric", or "All".
3. Extract CC range from standard notation (e.g. `<= 155` means from 1 to 155). 
4. Include/Exclude specific manufacturers if mentioned (e.g. `Others` means exclude explicit makes).
5. If segment is `SC/EV` or requests duplicating the row for Petrol & Electric, set `duplication_required` to true and output ONE segment that acts as the template. If duplication_required is true, just output ONE segment with Category Scooter and Fuel Petrol, the pipeline will duplicate it for Electric.
6. Provide a confidence score between 0.0 and 1.0.

Only return valid JSON matching the schema.
"""

    try:
        response = litellm.completion(
            model=model_name,
            messages=[{"role": "user", "content": sys_prompt}],
            response_format=LLMResponseModel,
            temperature=0.0
        )
        
        raw_json = response.choices[0].message.content
        parsed_llm = LLMResponseModel.model_validate_json(raw_json)
        
        results = []
        for s in parsed_llm.segments:
            cc = None
            if s.cc_range_from is not None or s.cc_range_to is not None:
                cc = {"from": s.cc_range_from, "to": s.cc_range_to}

            
            # Reconstruct duplication logic if the LLM flagged it
            if s.duplication_required:
                results.append(ParsedSegment(
                    category=s.category, cc_range=cc, fuel="Petrol",
                    manufacturers={"include": s.include_makes, "exclude": s.exclude_makes},
                    duplication_required=True, confidence_score=parsed_llm.confidence_score,
                    canonical_form=original, original_input=original, warnings=["[LLM Fallback Auto-Correction]"]
                ))
            else:
                results.append(ParsedSegment(
                    category=s.category, cc_range=cc, fuel=s.fuel,
                    manufacturers={"include": s.include_makes, "exclude": s.exclude_makes},
                    duplication_required=False, confidence_score=parsed_llm.confidence_score,
                    canonical_form=original, original_input=original, warnings=["[LLM Fallback Auto-Correction]"]
                ))
                
        # If the LLM is confident (> 0.90), we trust it over falling to HITL
        if parsed_llm.confidence_score >= 0.90:
             return results
        return None
        
    except Exception as e:
        print(f"LLM Fallback failed for '{original}': {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# HITL QUEUE
# ═══════════════════════════════════════════════════════════════════════════════

CONFIDENCE_THRESHOLD = 0.95

class HITLQueue:
    """Human-In-The-Loop queue for low-confidence parsing results."""

    def __init__(self):
        self.queue: List[Dict[str, Any]] = []
        self.auto_approved: int = 0
        self.manual_review: int = 0

    def submit(self, parsed: ParsedSegment, row_context: dict) -> bool:
        """
        Submit a parsed result for routing.
        Returns True if auto-approved, False if sent to HITL queue.
        """
        if parsed.confidence_score >= CONFIDENCE_THRESHOLD:
            self.auto_approved += 1
            return True
        else:
            self.queue.append({
                "parsed": parsed.to_dict(),
                "context": row_context,
                "reason": parsed.warnings or ["Low confidence"],
            })
            self.manual_review += 1
            return False

    def export_queue(self, filepath: str):
        """Export HITL queue to JSON for manual review."""
        with open(filepath, 'w') as f:
            json.dump({
                "total_items": len(self.queue),
                "items": self.queue,
            }, f, indent=2)

    def summary(self) -> str:
        total = self.auto_approved + self.manual_review
        return (
            f"  Auto-approved: {self.auto_approved}/{total} "
            f"({self.auto_approved/total*100:.1f}%)\n"
            f"  Manual review: {self.manual_review}/{total} "
            f"({self.manual_review/total*100:.1f}%)"
        ) if total > 0 else "  No segments processed"


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE: BATCH PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_segment_batch(segments: list, context: str = "1plus1") -> list:
    """Parse a batch of segments, returning list of (input, [ParsedSegment])."""
    results = []
    for seg in segments:
        parsed = parse_segment(seg, sheet_context=context)
        results.append((seg, parsed))
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# CLI — standalone testing
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    test_segments = [
        # Cluster-level (1+1/SATP)
        "SC/EV",
        "MC <= 180 Hero/Honda",
        "MC <= 180 Hero/Honda/TVS",
        "MC <= 180 Others",
        "MC_180-350_RE",
        "MC_180-350_HONDA/JAWA/Avenger",
        "MC_180-350_Other than RE",
        "MC_180-350_Others",
        "MC>350",
        # Make-specific (1+5, 5+5)
        "MC <=155",
        "MC >155",
        "SCOOTER",
        "< =350",
        "> 350",
        "3-7 KW",
        "< 3 KW",
        "> 3 KW",
        "All",
        "EV",
        "SCOOTER/EV",
        "SCOOTER/MC",
        "MC",
        # SAOD
        "MC <155",
        "MC>155",
        "RE",
        "SC",
        "SC_EV",
    ]

    print("=" * 70)
    print("  SEGMENT PARSER — Test Suite")
    print("=" * 70)

    hitl = HITLQueue()

    for seg in test_segments:
        results = parse_segment(seg)
        for r in results:
            approved = hitl.submit(r, {"segment": seg})
            status = "✅" if approved else "⚠️ HITL"
            dup = " [DUP]" if r.duplication_required else ""
            cc = f"CC={r.cc_range['from']}-{r.cc_range['to']}" if r.cc_range else "CC=—"
            make = r.to_portal_make()
            print(f"  {status} '{seg}'{dup}")
            print(f"       → {r.category}/{r.fuel} | {cc} | Make={make} | Conf={r.confidence_score}")

    print(f"\n{'=' * 70}")
    print("  CONFIDENCE REPORT")
    print(f"{'=' * 70}")
    print(hitl.summary())

    if hitl.queue:
        print(f"\n  Items requiring review:")
        for item in hitl.queue:
            print(f"    - '{item['parsed']['original_input']}': {item['reason']}")
