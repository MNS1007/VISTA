#!/usr/bin/env python3
"""
Risk Scorer for OSHA Hazard Analysis

Computes Site Risk Score (0-100) based purely on real OSHA incident data.
No external severity inputs — the score is 100% derived from what actually
happened to workers historically in OSHA records.

Score Components:
- Frequency Score (max 25 pts): How often does this hazard appear?
- Fatality Score (max 35 pts): What fraction of incidents were fatal?
- Severity Score (max 25 pts): Average days away from work
- Serious Case Rate (max 15 pts): Fraction with 30+ days away
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def get_project_root() -> Path:
    """Get the project root directory (utils folder)."""
    return Path(__file__).parent.parent


def get_category_filter(category: str) -> str:
    """
    Convert hazard category to SQL WHERE clause filter.
    Returns SQL condition for event_title_pred and source_title_pred matching.
    """
    category_lower = category.lower()
    
    if "fall" in category_lower:
        return "event_title_pred LIKE '%fall%'"
    elif "electric" in category_lower:
        return """(event_title_pred LIKE '%contact with electric%'
            OR event_title_pred LIKE '%contact with wiring%'
            OR source_title_pred LIKE '%electric%'
            OR source_title_pred LIKE '%wiring%'
            OR source_title_pred LIKE '%power line%'
            OR nar_what_happened LIKE '%electrocuted%'
            OR nar_what_happened LIKE '%electric shock%')"""
    elif "struck" in category_lower:
        return "event_title_pred LIKE '%struck%'"
    elif "caught" in category_lower or "compress" in category_lower:
        return "(event_title_pred LIKE '%caught%' OR event_title_pred LIKE '%compress%')"
    elif "chemical" in category_lower:
        return "(event_title_pred LIKE '%expos%' AND (source_title_pred LIKE '%chemical%' OR source_title_pred LIKE '%toxic%'))"
    elif "slip" in category_lower or "trip" in category_lower:
        return "(event_title_pred LIKE '%fall on same level%' OR event_title_pred LIKE '%slip%' OR event_title_pred LIKE '%trip%')"
    elif "fire" in category_lower:
        return "(event_title_pred LIKE '%fire%' OR event_title_pred LIKE '%explosion%' OR event_title_pred LIKE '%burn%')"
    else:
        # Default: search in event_title_pred
        return f"event_title_pred LIKE '%{category}%'"


def get_grade(score: float) -> Tuple[str, str]:
    """
    Convert score to letter grade and explanation.
    Returns (grade, explanation)
    """
    if score <= 20:
        return ("A", "A: 0-20 risk range. Low risk site. Standard safety protocols sufficient.")
    elif score <= 40:
        return ("B", "B: 21-40 risk range. Moderate risk. Enhanced safety measures recommended.")
    elif score <= 60:
        return ("C", "C: 41-60 risk range. Elevated risk. Regular safety audits required.")
    elif score <= 80:
        return ("D", "D: 61-80 risk range. High-risk site. Immediate corrective action recommended.")
    else:
        return ("F", "F: 81-100 risk range. Critical risk. Site shutdown may be required until hazards are mitigated.")


def get_recommendation(score: float) -> str:
    """Generate recommendation based on score."""
    if score <= 20:
        return "Low-risk site. Standard safety protocols sufficient."
    elif score <= 40:
        return "Moderate-risk site. Enhanced safety measures recommended."
    elif score <= 60:
        return "Elevated-risk site. Regular safety audits and training required."
    elif score <= 80:
        return "High-risk site. Daily safety briefings required. Immediate corrective action needed."
    else:
        return "Critical-risk site. Consider site shutdown until hazards are mitigated. Emergency safety protocols required."


def query_frequency(cursor: sqlite3.Cursor, category_filter: str) -> int:
    """Query 1: Count total incidents matching this hazard category."""
    query = f"SELECT COUNT(*) FROM incidents WHERE {category_filter}"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else 0


def query_fatality_count(cursor: sqlite3.Cursor, category_filter: str) -> int:
    """Query 2: Count fatal incidents for this hazard category."""
    query = f"SELECT COUNT(*) FROM incidents WHERE {category_filter} AND incident_outcome = 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else 0


def query_avg_dafw(cursor: sqlite3.Cursor, category_filter: str) -> float:
    """Query 3: Average dafw_num_away for incidents with dafw > 0."""
    query = f"SELECT AVG(dafw_num_away) FROM incidents WHERE {category_filter} AND dafw_num_away > 0"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result and result[0] is not None else 0.0


def query_severe_count(cursor: sqlite3.Cursor, category_filter: str) -> int:
    """Query 4: Count incidents with 30+ days away from work."""
    query = f"SELECT COUNT(*) FROM incidents WHERE {category_filter} AND dafw_num_away >= 30"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else 0


def compute_hazard_score(
    frequency_count: int,
    fatal_count: int,
    avg_dafw: float,
    severe_count: int
) -> Tuple[float, Dict[str, float]]:
    """
    Compute hazard score 0-100 purely from OSHA historical data.
    
    No external inputs — the score is derived entirely from what actually
    happened to workers in real OSHA incident records.
    
    Args:
        frequency_count: Total incidents matching this hazard
        fatal_count: Number of fatal incidents
        avg_dafw: Average days away from work
        severe_count: Number of incidents with 30+ days away
        
    Returns:
        Tuple of (final_score, score_components dict)
    """
    if frequency_count == 0:
        return 0.0, {
            'frequency_score': 0.0,
            'fatality_score': 0.0,
            'severity_score': 0.0,
            'severe_case_score': 0.0
        }
    
    # COMPONENT 1 — Frequency Score (max 25 points)
    # How often does this hazard type appear in OSHA data?
    frequency_score = min(frequency_count / 500.0, 1.0) * 25.0
    
    # COMPONENT 2 — Fatality Score (max 35 points)
    # What fraction of these incidents killed the worker?
    fatality_rate = fatal_count / frequency_count
    fatality_score = fatality_rate * 35.0
    
    # COMPONENT 3 — Severity Score (max 25 points)
    # How many days did injured workers lose on average?
    # 90 days = roughly a quarter year = full score
    severity_score = min(avg_dafw / 90.0, 1.0) * 25.0
    
    # COMPONENT 4 — Serious Case Rate (max 15 points)
    # What fraction of cases were severe (30+ days away from work)?
    severe_rate = severe_count / frequency_count
    severe_case_score = severe_rate * 15.0
    
    # Raw score (0-100)
    raw_score = frequency_score + fatality_score + severity_score + severe_case_score
    final_score = round(min(raw_score, 100.0), 1)
    
    score_components = {
        'frequency_score': round(frequency_score, 1),
        'fatality_score': round(fatality_score, 1),
        'severity_score': round(severity_score, 1),
        'severe_case_score': round(severe_case_score, 1)
    }
    
    return final_score, score_components


def compute_site_risk(
    hazard_registry: Dict[str, Dict[str, str]],
    db_path: Optional[str] = None
) -> Dict:
    """
    Compute Site Risk Score based purely on OSHA incident data.
    
    The score is 100% derived from historical incident records — no external
    severity inputs or multipliers are used.
    
    Args:
        hazard_registry: Dict mapping hazard_id to {label, category}
                        (severity field is ignored if present)
        db_path: Optional path to SQLite database (defaults to osha_incidents.db)
        
    Returns:
        Dict with score, grade, breakdown, top_concern, recommendation
    """
    if db_path is None:
        db_path = str(get_project_root() / 'osha_incidents.db')
    
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    breakdown = []
    hazard_scores = []
    
    # Process each hazard in the registry
    for hazard_id, hazard_data in hazard_registry.items():
        label = hazard_data.get('label', '')
        category = hazard_data.get('category', '')
        
        # Get category filter
        category_filter = get_category_filter(category)
        
        # Run 4 queries against OSHA data
        frequency_count = query_frequency(cursor, category_filter)
        fatal_count = query_fatality_count(cursor, category_filter)
        avg_dafw = query_avg_dafw(cursor, category_filter)
        severe_count = query_severe_count(cursor, category_filter)
        
        # Calculate rates
        fatality_rate = fatal_count / frequency_count if frequency_count > 0 else 0.0
        severe_rate = severe_count / frequency_count if frequency_count > 0 else 0.0
        
        # Compute score purely from OSHA data
        final_score, score_components = compute_hazard_score(
            frequency_count, fatal_count, avg_dafw, severe_count
        )
        
        hazard_scores.append(final_score)
        
        breakdown.append({
            'hazard_id': hazard_id,
            'label': label,
            'category': category,
            'frequency_count': frequency_count,
            'fatal_count': fatal_count,
            'fatality_rate': round(fatality_rate, 3),
            'avg_dafw': round(avg_dafw, 1),
            'severe_rate': round(severe_rate, 3),
            'final_score': final_score,
            'score_components': score_components
        })
    
    conn.close()
    
    # Sort breakdown by risk score (highest first)
    breakdown.sort(key=lambda x: x['final_score'], reverse=True)
    
    # Get top 5 hazards
    top_5_hazards = breakdown[:5]
    
    # Compute composite site score
    if not hazard_scores:
        site_score = 0.0
    elif len(hazard_scores) == 1:
        site_score = hazard_scores[0]
    else:
        # Worst hazard dominates: highest * 0.6 + mean(others) * 0.4
        sorted_scores = sorted(hazard_scores, reverse=True)
        highest = sorted_scores[0]
        others = sorted_scores[1:]
        mean_others = sum(others) / len(others) if others else 0.0
        site_score = highest * 0.6 + mean_others * 0.4
    
    site_score = round(site_score, 1)
    
    # Get grade
    grade, grade_explanation = get_grade(site_score)
    
    # Find top concern (first in sorted breakdown)
    top_concern_entry = breakdown[0] if breakdown else None
    top_concern = top_concern_entry['label'] if top_concern_entry else "None"
    top_concern_stats = (
        f"{top_concern_entry['frequency_count']:,} similar incidents in OSHA data. "
        f"{top_concern_entry['fatal_count']} fatalities. "
        f"Avg {top_concern_entry['avg_dafw']} days away from work."
    ) if top_concern_entry else ""
    
    # Get recommendation
    recommendation = get_recommendation(site_score)
    
    return {
        'score': site_score,
        'grade': grade,
        'breakdown': breakdown,
        'top_5_hazards': top_5_hazards,
        'top_concern': top_concern,
        'top_concern_stats': top_concern_stats,
        'recommendation': recommendation,
        'grade_explanation': grade_explanation
    }


def format_risk_report(result: Dict) -> str:
    """Format risk score result as a readable report."""
    lines = []
    lines.append("=" * 70)
    lines.append("SITE RISK ASSESSMENT (Based on OSHA Historical Data)")
    lines.append("=" * 70)
    lines.append(f"\nOverall Risk Score: {result['score']}/100")
    lines.append(f"Grade: {result['grade']}")
    lines.append(f"\n{result['grade_explanation']}")
    lines.append(f"\nRecommendation: {result['recommendation']}")
    lines.append(f"\nTop Concern: {result['top_concern']}")
    lines.append(f"  {result['top_concern_stats']}")
    
    # Show top 5 hazards
    lines.append("\n" + "-" * 70)
    lines.append("TOP 5 HAZARDS (Ranked by Risk Score)")
    lines.append("-" * 70)
    
    top_5 = result.get('top_5_hazards', result['breakdown'][:5])
    for rank, hazard in enumerate(top_5, 1):
        lines.append(f"\n#{rank}. {hazard['label']} ({hazard['category']}) — Score: {hazard['final_score']}/100")
        sc = hazard['score_components']
        lines.append(f"    Frequency:     {sc['frequency_score']:5.1f}/25 pts  ({hazard['frequency_count']:,} incidents)")
        lines.append(f"    Fatality Rate: {sc['fatality_score']:5.1f}/35 pts  ({hazard['fatality_rate']*100:.1f}% fatal)")
        lines.append(f"    Severity:      {sc['severity_score']:5.1f}/25 pts  (avg {hazard['avg_dafw']} days away)")
        lines.append(f"    Serious Cases: {sc['severe_case_score']:5.1f}/15 pts  ({hazard['severe_rate']*100:.1f}% with 30+ days)")
    
    # Show remaining hazards if any
    remaining = result['breakdown'][5:]
    if remaining:
        lines.append("\n" + "-" * 70)
        lines.append("OTHER HAZARDS")
        lines.append("-" * 70)
        for hazard in remaining:
            lines.append(f"\n{hazard['label']} ({hazard['category']}) — Score: {hazard['final_score']}/100")
            lines.append(f"    {hazard['frequency_count']:,} incidents | {hazard['fatality_rate']*100:.1f}% fatal | avg {hazard['avg_dafw']} days away")
    
    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


def main():
    """Test the risk scorer with sample hazard registry."""
    print("Testing Risk Scorer (OSHA Data Only)...")
    print()
    
    # Sample hazard registry with all supported categories
    test_registry = {
        "hazard_001": {
            "label": "Floor Hole",
            "category": "Fall Hazard"
        },
        "hazard_002": {
            "label": "Exposed Wiring",
            "category": "Electrical Hazard"
        },
        "hazard_003": {
            "label": "Debris on Floor",
            "category": "Slip/Trip"
        },
        "hazard_004": {
            "label": "Unsecured Load",
            "category": "Struck By"
        },
        "hazard_005": {
            "label": "Unguarded Machine",
            "category": "Caught In"
        },
        "hazard_006": {
            "label": "Chemical Storage",
            "category": "Chemical Hazard"
        },
        "hazard_007": {
            "label": "Welding Area",
            "category": "Fire Hazard"
        }
    }
    
    try:
        result = compute_site_risk(test_registry)
        print(format_risk_report(result))
        
        print("\n" + "=" * 70)
        print("JSON OUTPUT:")
        print("=" * 70)
        import json
        print(json.dumps(result, indent=2))
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run the indexer first to create the database.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
