#!/usr/bin/env python3
"""
RAG Retriever for OSHA Incident Narratives

Searches SQLite database with FTS5 to find real OSHA incidents matching
a given hazard label, returning narratives for use in hazard warnings.
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Optional, Set


def get_project_root() -> Path:
    """Get the project root directory (utils folder)."""
    return Path(__file__).parent.parent


def get_db_path() -> Path:
    """Get the path to the SQLite database."""
    return get_project_root() / 'osha_incidents.db'


def format_outcome(incident_outcome: Optional[int], dafw_num_away: int, djtr_num_tr: int) -> str:
    """Convert incident_outcome integer to human-readable string."""
    if incident_outcome == 1:
        return "FATAL"
    elif incident_outcome == 2:
        if dafw_num_away > 0:
            return f"{dafw_num_away} days away from work"
        else:
            return "Days away from work"
    elif incident_outcome == 3:
        if djtr_num_tr > 0:
            return f"{djtr_num_tr} days job transfer/restriction"
        else:
            return "Job transfer/restriction"
    elif incident_outcome == 4:
        return "Other recordable case"
    else:
        return "Unknown"


def get_category_expansion(category: Optional[str]) -> List[str]:
    """Get additional search terms based on hazard category."""
    if category is None:
        return []
    
    category = category.lower()
    expansions = []
    
    if "fall" in category:
        expansions.extend([
            "event_title_pred LIKE '%fall%'",
            "source_title_pred LIKE '%ladder%'",
            "source_title_pred LIKE '%scaffold%'",
            "source_title_pred LIKE '%roof%'"
        ])
    elif "electric" in category:
        expansions.extend([
            "event_title_pred LIKE '%contact with electric%'",
            "event_title_pred LIKE '%contact with wiring%'",
            "source_title_pred LIKE '%electric%'",
            "source_title_pred LIKE '%wiring%'",
            "source_title_pred LIKE '%power line%'",
            "nar_what_happened LIKE '%electrocuted%'",
            "nar_what_happened LIKE '%electric shock%'"
        ])
    elif "struck" in category or "hit" in category:
        expansions.extend([
            "event_title_pred LIKE '%struck%'",
            "event_title_pred LIKE '%hit%'"
        ])
    elif "caught" in category or "compress" in category:
        expansions.extend([
            "event_title_pred LIKE '%caught%'",
            "event_title_pred LIKE '%compress%'"
        ])
    elif "chemical" in category:
        expansions.extend([
            "event_title_pred LIKE '%expos%'",
            "source_title_pred LIKE '%chemical%'"
        ])
    elif "slip" in category or "trip" in category:
        expansions.extend([
            "event_title_pred LIKE '%slip%'",
            "event_title_pred LIKE '%trip%'",
            "event_title_pred LIKE '%same level%'"
        ])
    
    return expansions


def build_fts_query(hazard_label: str) -> str:
    """
    Build FTS5 query string from hazard label.
    FTS5 uses space-separated terms, and we want to search across all fields.
    """
    # Clean and split the hazard label into search terms
    terms = hazard_label.lower().strip().split()
    # Join with OR to match any term, and use * for prefix matching
    query_terms = [f"{term}*" for term in terms if term]
    return " OR ".join(query_terms) if query_terms else hazard_label


def retrieve_narratives(
    hazard_label: str,
    category: Optional[str] = None,
    k: int = 3
) -> List[Dict]:
    """
    Retrieve OSHA incident narratives matching a hazard label.
    
    Args:
        hazard_label: Text description of the hazard (e.g., "floor hole")
        category: Optional hazard category for search expansion
        k: Number of results to return
        
    Returns:
        List of dicts, each containing incident narrative data
    """
    db_path = get_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Step 1: FTS5 search across narrative fields
    fts_query = build_fts_query(hazard_label)
    fts_rowids: Set[int] = set()
    
    try:
        # FTS5 search - rank is bm25 relevance (lower/more negative = more relevant)
        cursor.execute("""
            SELECT rowid, rank
            FROM incidents_fts
            WHERE incidents_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (fts_query, k * 3))  # Get more candidates for deduplication
        
        for row in cursor.fetchall():
            fts_rowids.add(row['rowid'])
    except sqlite3.OperationalError as e:
        # If FTS5 query fails, try simpler approach
        print(f"Warning: FTS5 query failed: {e}")
        # Fallback: search individual fields with LIKE
        cursor.execute("""
            SELECT id FROM incidents
            WHERE nar_what_happened LIKE ? OR
                  nar_before_incident LIKE ? OR
                  incident_location LIKE ? OR
                  nar_injury_illness LIKE ? OR
                  nar_object_substance LIKE ? OR
                  incident_description LIKE ?
            LIMIT ?
        """, (f"%{hazard_label}%",) * 6 + (k * 3,))
        for row in cursor.fetchall():
            fts_rowids.add(row['id'])
    
    # Step 2: Direct LIKE searches on event_title_pred and source_title_pred
    like_rowids: Set[int] = set()
    search_pattern = f"%{hazard_label}%"
    
    cursor.execute("""
        SELECT id FROM incidents
        WHERE event_title_pred LIKE ? OR source_title_pred LIKE ?
        LIMIT ?
    """, (search_pattern, search_pattern, k * 3))
    
    for row in cursor.fetchall():
        like_rowids.add(row['id'])
    
    # Step 3: Category-based expansion
    category_rowids: Set[int] = set()
    if category:
        expansions = get_category_expansion(category)
        if expansions:
            expansion_query = " OR ".join(expansions)
            cursor.execute(f"""
                SELECT id FROM incidents
                WHERE {expansion_query}
                LIMIT ?
            """, (k * 3,))
            for row in cursor.fetchall():
                category_rowids.add(row['id'])
    
    # Combine all rowids and deduplicate
    all_rowids = fts_rowids | like_rowids | category_rowids
    
    if not all_rowids:
        conn.close()
        return []
    
    # Step 4: Join to incidents table and get full data
    # Sort by: fatals first, then by dafw_num_away DESC
    placeholders = ','.join('?' * len(all_rowids))
    cursor.execute(f"""
        SELECT 
            id, establishment_name, city, state, naics_code,
            year_filing_for, date_of_incident, incident_outcome,
            dafw_num_away, djtr_num_tr, type_of_incident, job_description,
            nar_what_happened, nar_before_incident, incident_location,
            nar_injury_illness, nar_object_substance, incident_description,
            nature_title_pred, part_title_pred, event_title_pred,
            source_title_pred, sec_source_title_pred
        FROM incidents
        WHERE id IN ({placeholders})
        ORDER BY 
            CASE WHEN incident_outcome = 1 THEN 0 ELSE 1 END,
            dafw_num_away DESC
        LIMIT ?
    """, list(all_rowids) + [k])
    
    results = []
    for row in cursor.fetchall():
        incident_outcome = row['incident_outcome']
        dafw_num_away = row['dafw_num_away'] or 0
        djtr_num_tr = row['djtr_num_tr'] or 0
        
        # Calculate relevance score (simple heuristic)
        # Higher score for: fatals, high dafw, matches in key fields
        relevance_score = 0.5  # Base score
        if incident_outcome == 1:
            relevance_score += 0.3  # Fatal cases are highly relevant
        if dafw_num_away > 30:
            relevance_score += 0.1  # Very severe injuries
        elif dafw_num_away > 0:
            relevance_score += 0.05  # Any days away
        if row['id'] in fts_rowids:
            relevance_score += 0.1  # Matched in narrative text
        if row['id'] in like_rowids:
            relevance_score += 0.05  # Matched in OSHA classifications
        
        relevance_score = min(1.0, relevance_score)  # Cap at 1.0
        
        result = {
            'what_happened': row['nar_what_happened'] or '',
            'injury_description': row['nar_injury_illness'] or '',
            'object_involved': row['nar_object_substance'] or '',
            'location': row['incident_location'] or '',
            'outcome': format_outcome(incident_outcome, dafw_num_away, djtr_num_tr),
            'dafw_days': dafw_num_away,
            'event_type': row['event_title_pred'] or '',
            'source': row['source_title_pred'] or '',
            'nature_of_injury': row['nature_title_pred'] or '',
            'body_part': row['part_title_pred'] or '',
            'year': row['year_filing_for'],
            'is_fatal': (incident_outcome == 1),
            'relevance_score': relevance_score
        }
        results.append(result)
    
    conn.close()
    return results


def format_for_display(narratives: List[Dict]) -> str:
    """
    Format narratives as a readable string for display.
    
    Args:
        narratives: List of narrative dicts from retrieve_narratives()
        
    Returns:
        Formatted string
    """
    if not narratives:
        return "No matching incidents found."
    
    lines = ["⚠️ Real OSHA incidents matching this hazard:"]
    
    for i, nar in enumerate(narratives, 1):
        year = nar.get('year', 'Unknown')
        outcome = nar.get('outcome', 'Unknown')
        what_happened = nar.get('what_happened', '')
        
        # Truncate what_happened if too long
        if len(what_happened) > 150:
            what_happened = what_happened[:147] + "..."
        
        # Build injury summary
        injury_parts = []
        if nar.get('nature_of_injury'):
            injury_parts.append(nar['nature_of_injury'])
        if nar.get('body_part'):
            injury_parts.append(nar['body_part'])
        injury_summary = ", ".join(injury_parts) if injury_parts else "Injury details not available"
        
        line = f" {i}. [{year} | {outcome}] {what_happened}"
        if injury_summary:
            line += f" → {injury_summary}"
        
        lines.append(line)
    
    return "\n".join(lines)


def main():
    """Test the retriever with a sample query."""
    print("Testing RAG Retriever...")
    print("=" * 60)
    
    try:
        narratives = retrieve_narratives("floor hole", category="Fall Hazard", k=5)
        
        print(f"\nFound {len(narratives)} incidents\n")
        print(format_for_display(narratives))
        
        print("\n" + "=" * 60)
        print("\nDetailed results:")
        for i, nar in enumerate(narratives, 1):
            print(f"\n{i}. Relevance: {nar['relevance_score']:.2f}")
            print(f"   Year: {nar['year']}")
            print(f"   Outcome: {nar['outcome']}")
            print(f"   Event Type: {nar['event_type']}")
            print(f"   Source: {nar['source']}")
            print(f"   What Happened: {nar['what_happened'][:200]}...")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
