#!/usr/bin/env python3
"""
OSHA Statistics Module

Pre-computes headline statistics for VESTA hazard categories,
providing instant citation-ready stats for UI display.
"""

import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def get_project_root() -> Path:
    """Get the project root directory (parent of vesta)."""
    return Path(__file__).parent.parent.parent


def get_db_path() -> Path:
    """Get the path to the SQLite database."""
    return get_project_root() / 'osha_incidents.db'


def get_cache_path() -> Path:
    """Get the path to the stats cache JSON file."""
    return get_project_root() / 'osha_stats_cache.json'


def get_category_filter(category: str) -> str:
    """
    Convert hazard category to SQL WHERE clause filter.
    Returns SQL condition for event_title_pred matching.
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
    elif "caught" in category_lower or "between" in category_lower:
        return "(event_title_pred LIKE '%caught%' OR event_title_pred LIKE '%compress%')"
    elif "slip" in category_lower or "trip" in category_lower:
        return "(event_title_pred LIKE '%fall on same level%' OR event_title_pred LIKE '%slip%')"
    elif "fire" in category_lower or "explosion" in category_lower:
        return "(event_title_pred LIKE '%fire%' OR event_title_pred LIKE '%explosion%')"
    else:
        # Default: search in event_title_pred
        return f"event_title_pred LIKE '%{category}%'"


def compute_category_stats(cursor: sqlite3.Cursor, category: str, category_filter: str) -> Dict:
    """
    Compute all statistics for a given hazard category.
    
    Returns:
        Dict with all computed statistics
    """
    stats = {}
    
    # 1. Total count
    cursor.execute(f"SELECT COUNT(*) FROM incidents WHERE {category_filter}")
    stats['total_count'] = cursor.fetchone()[0]
    
    if stats['total_count'] == 0:
        # Return empty stats if no incidents
        return {
            'total_count': 0,
            'fatal_count': 0,
            'dafw_count': 0,
            'avg_dafw': 0.0,
            'max_dafw': 0,
            'pct_fatal': 0.0,
            'top_sources': [],
            'top_body_parts': [],
            'year_breakdown': {}
        }
    
    # 2. Fatal count
    cursor.execute(f"""
        SELECT COUNT(*) FROM incidents 
        WHERE {category_filter} AND incident_outcome = 1
    """)
    stats['fatal_count'] = cursor.fetchone()[0]
    
    # 3. DAFW count (incident_outcome = 2)
    cursor.execute(f"""
        SELECT COUNT(*) FROM incidents 
        WHERE {category_filter} AND incident_outcome = 2
    """)
    stats['dafw_count'] = cursor.fetchone()[0]
    
    # 4. Average dafw (only for incidents with dafw > 0)
    cursor.execute(f"""
        SELECT AVG(dafw_num_away) FROM incidents 
        WHERE {category_filter} AND dafw_num_away > 0
    """)
    result = cursor.fetchone()[0]
    stats['avg_dafw'] = round(result, 1) if result and result is not None else 0.0
    
    # 5. Max dafw
    cursor.execute(f"""
        SELECT MAX(dafw_num_away) FROM incidents 
        WHERE {category_filter}
    """)
    result = cursor.fetchone()[0]
    stats['max_dafw'] = result if result is not None else 0
    
    # 6. Percentage fatal
    stats['pct_fatal'] = round((stats['fatal_count'] / stats['total_count']) * 100, 1) if stats['total_count'] > 0 else 0.0
    
    # 7. Top sources (what objects/surfaces cause the most harm)
    cursor.execute(f"""
        SELECT source_title_pred, COUNT(*) as n
        FROM incidents
        WHERE {category_filter} AND source_title_pred IS NOT NULL AND source_title_pred != ''
        GROUP BY source_title_pred
        ORDER BY n DESC
        LIMIT 3
    """)
    stats['top_sources'] = [{'source': row[0], 'count': row[1]} for row in cursor.fetchall()]
    
    # 8. Top body parts
    cursor.execute(f"""
        SELECT part_title_pred, COUNT(*) as n
        FROM incidents
        WHERE {category_filter} AND part_title_pred IS NOT NULL AND part_title_pred != ''
        GROUP BY part_title_pred
        ORDER BY n DESC
        LIMIT 3
    """)
    stats['top_body_parts'] = [{'body_part': row[0], 'count': row[1]} for row in cursor.fetchall()]
    
    # 9. Year breakdown
    cursor.execute(f"""
        SELECT year_filing_for, COUNT(*) as n
        FROM incidents
        WHERE {category_filter}
        GROUP BY year_filing_for
        ORDER BY year_filing_for
    """)
    stats['year_breakdown'] = {str(row[0]): row[1] for row in cursor.fetchall() if row[0] is not None}
    
    return stats


def build_stats_cache(db_path: Optional[str] = None) -> Dict[str, Dict]:
    """
    Build statistics cache for all hazard categories.
    
    Args:
        db_path: Optional path to SQLite database
        
    Returns:
        Dict mapping category names to statistics
    """
    if db_path is None:
        db_path = str(get_db_path())
    
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Define all categories to compute stats for
    categories = [
        "Fall Hazard",
        "Electrical Hazard",
        "Struck By",
        "Caught In/Between",
        "Slip/Trip",
        "Fire/Explosion"
    ]
    
    all_stats = {}
    
    print("Building statistics cache...")
    print("=" * 60)
    
    for category in categories:
        print(f"Processing {category}...", end=" ")
        category_filter = get_category_filter(category)
        stats = compute_category_stats(cursor, category, category_filter)
        all_stats[category] = stats
        print(f"{stats['total_count']:,} incidents")
    
    conn.close()
    
    # Save to JSON cache
    cache_path = get_cache_path()
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(all_stats, f, indent=2)
    
    print("=" * 60)
    print(f"Cache saved to {cache_path}")
    
    return all_stats


def get_all_stats(db_path: Optional[str] = None, use_cache: bool = True) -> Dict[str, Dict]:
    """
    Get all statistics for all categories.
    Loads from cache if available, otherwise queries live.
    
    Args:
        db_path: Optional path to SQLite database
        use_cache: Whether to use cached data if available
        
    Returns:
        Dict mapping category names to statistics
    """
    cache_path = get_cache_path()
    
    if use_cache and cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            # Cache is corrupted or unreadable, rebuild
            pass
    
    # Cache doesn't exist or is invalid, build it
    return build_stats_cache(db_path)


def get_headline_stat(category: str, db_path: Optional[str] = None, use_cache: bool = True) -> str:
    """
    Get a punchy headline statistic string for a category.
    
    Args:
        category: Hazard category name
        db_path: Optional path to SQLite database
        use_cache: Whether to use cached data if available
        
    Returns:
        Formatted headline string
    """
    all_stats = get_all_stats(db_path, use_cache)
    
    if category not in all_stats:
        return f"No statistics available for {category}."
    
    stats = all_stats[category]
    
    if stats['total_count'] == 0:
        return f"No {category} incidents found in dataset."
    
    # Build the headline string
    parts = []
    
    # Main stats
    parts.append(f"{stats['total_count']:,} {category} incidents recorded.")
    parts.append(f"{stats['fatal_count']} fatalities ({stats['pct_fatal']:.1f}%).")
    
    if stats['avg_dafw'] > 0:
        parts.append(f"Workers averaged {stats['avg_dafw']:.0f} days away from work.")
    
    # Top sources
    if stats['top_sources']:
        source_names = [s['source'] for s in stats['top_sources']]
        if len(source_names) >= 2:
            parts.append(f"Most common causes: {source_names[0]}, {source_names[1]}.")
        elif len(source_names) == 1:
            parts.append(f"Most common cause: {source_names[0]}.")
    
    # Top body parts
    if stats['top_body_parts']:
        body_part_names = [b['body_part'] for b in stats['top_body_parts']]
        if len(body_part_names) >= 1:
            parts.append(f"Most affected: {body_part_names[0]}.")
    
    return " ".join(parts)


def format_detailed_stats(category: str, db_path: Optional[str] = None, use_cache: bool = True) -> str:
    """
    Format detailed statistics for a category as a readable report.
    
    Args:
        category: Hazard category name
        db_path: Optional path to SQLite database
        use_cache: Whether to use cached data if available
        
    Returns:
        Formatted detailed report string
    """
    all_stats = get_all_stats(db_path, use_cache)
    
    if category not in all_stats:
        return f"No statistics available for {category}."
    
    stats = all_stats[category]
    
    if stats['total_count'] == 0:
        return f"No {category} incidents found in dataset."
    
    lines = []
    lines.append(f"{category} Statistics")
    lines.append("=" * 60)
    lines.append(f"Total Incidents: {stats['total_count']:,}")
    lines.append(f"Fatalities: {stats['fatal_count']} ({stats['pct_fatal']:.1f}%)")
    lines.append(f"Days Away from Work Cases: {stats['dafw_count']:,}")
    lines.append(f"Average Days Away: {stats['avg_dafw']:.1f} days")
    lines.append(f"Maximum Days Away: {stats['max_dafw']} days")
    
    if stats['top_sources']:
        lines.append("\nTop 3 Causes:")
        for i, source in enumerate(stats['top_sources'], 1):
            lines.append(f"  {i}. {source['source']}: {source['count']:,} incidents")
    
    if stats['top_body_parts']:
        lines.append("\nTop 3 Affected Body Parts:")
        for i, body_part in enumerate(stats['top_body_parts'], 1):
            lines.append(f"  {i}. {body_part['body_part']}: {body_part['count']:,} incidents")
    
    if stats['year_breakdown']:
        lines.append("\nYear Breakdown:")
        for year in sorted(stats['year_breakdown'].keys()):
            lines.append(f"  {year}: {stats['year_breakdown'][year]:,} incidents")
    
    return "\n".join(lines)


def main():
    """Build cache and print headline stats for each category."""
    print("OSHA Statistics Module")
    print("=" * 60)
    print()
    
    try:
        # Build the cache
        build_stats_cache()
        
        print()
        print("=" * 60)
        print("HEADLINE STATISTICS")
        print("=" * 60)
        print()
        
        categories = [
            "Fall Hazard",
            "Electrical Hazard",
            "Struck By",
            "Caught In/Between",
            "Slip/Trip",
            "Fire/Explosion"
        ]
        
        for category in categories:
            headline = get_headline_stat(category)
            print(f"{category}:")
            print(f"  {headline}")
            print()
        
        print("=" * 60)
        print("\nDETAILED STATISTICS (sample - Fall Hazard):")
        print("=" * 60)
        print(format_detailed_stats("Fall Hazard"))
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run the indexer first to create the database.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
