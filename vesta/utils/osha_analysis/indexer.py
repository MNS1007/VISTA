#!/usr/bin/env python3
"""
OSHA ITA Case Detail CSV Indexer

Reads OSHA ITA Case Detail CSV files and loads construction incidents
into SQLite with FTS5 full-text search.
"""

import csv
import sqlite3
import os
from pathlib import Path
from typing import List, Optional


def get_project_root() -> Path:
    """Get the project root directory (parent of vesta)."""
    return Path(__file__).parent.parent.parent


def safe_int(value: str) -> Optional[int]:
    """Convert string to int, treating blank/empty as None."""
    if not value or value.strip() == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_int_zero(value: str) -> int:
    """Convert string to int, treating blank/empty as 0."""
    if not value or value.strip() == '':
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def create_database(db_path: str) -> sqlite3.Connection:
    """Create the SQLite database with schema, FTS5 table, and indexes."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create main incidents table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY,
            establishment_name TEXT,
            city TEXT,
            state TEXT,
            naics_code TEXT,
            industry_description TEXT,
            year_filing_for INTEGER,
            date_of_incident TEXT,
            incident_outcome INTEGER,
            dafw_num_away INTEGER,
            djtr_num_tr INTEGER,
            type_of_incident INTEGER,
            job_description TEXT,
            nar_what_happened TEXT,
            nar_before_incident TEXT,
            incident_location TEXT,
            nar_injury_illness TEXT,
            nar_object_substance TEXT,
            incident_description TEXT,
            nature_title_pred TEXT,
            part_title_pred TEXT,
            event_title_pred TEXT,
            source_title_pred TEXT,
            sec_source_title_pred TEXT
        )
    """)
    
    # Create FTS5 virtual table for full-text search
    cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS incidents_fts USING fts5(
            nar_what_happened,
            nar_before_incident,
            incident_location,
            nar_injury_illness,
            nar_object_substance,
            incident_description,
            event_title_pred,
            source_title_pred,
            nature_title_pred,
            content='incidents',
            content_rowid='id'
        )
    """)
    
    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_incident_outcome ON incidents(incident_outcome)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_year ON incidents(year_filing_for)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event ON incidents(event_title_pred)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_naics ON incidents(naics_code)")
    
    conn.commit()
    return conn


def load_csv_files(conn: sqlite3.Connection, csv_files: List[str]) -> int:
    """
    Load CSV files into the database.
    
    Args:
        conn: SQLite connection
        csv_files: List of CSV filenames to load
        
    Returns:
        Number of rows inserted
    """
    cursor = conn.cursor()
    project_root = get_project_root()
    data_dir = project_root / 'data'
    
    insert_sql = """
        INSERT INTO incidents (
            id, establishment_name, city, state, naics_code, industry_description,
            year_filing_for, date_of_incident, incident_outcome, dafw_num_away,
            djtr_num_tr, type_of_incident, job_description,
            nar_what_happened, nar_before_incident, incident_location,
            nar_injury_illness, nar_object_substance, incident_description,
            nature_title_pred, part_title_pred, event_title_pred,
            source_title_pred, sec_source_title_pred
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    total_rows = 0
    
    for filename in csv_files:
        csv_path = data_dir / filename
        if not csv_path.exists():
            print(f"Warning: File not found: {csv_path}")
            continue
        
        print(f"Loading {filename}...")
        rows_in_file = 0
        
        # Try different encodings to handle various CSV file formats
        # CP1252 (Windows-1252) is common for CSV files from Windows systems
        encodings = ['utf-8', 'cp1252', 'latin-1']
        file_opened = False
        
        for encoding in encodings:
            try:
                with open(csv_path, 'r', encoding=encoding, errors='replace') as f:
                    reader = csv.DictReader(f)
                    # Test by reading first row to verify encoding works
                    first_row = next(reader, None)
                    if first_row is None:
                        # Empty file, skip
                        break
                    f.seek(0)  # Reset to beginning
                    reader = csv.DictReader(f)  # Recreate reader after seek
                    print(f"  Using encoding: {encoding}")
                    file_opened = True
                    
                    for row in reader:
                            # Filter: only keep rows where naics_code starts with "23"
                            naics_code = row.get('naics_code', '').strip()
                            if not naics_code.startswith('23'):
                                continue
                            
                            # Extract and convert values
                            incident_id = safe_int(row.get('id', ''))
                            if incident_id is None:
                                continue  # Skip rows without valid ID
                            
                            # Map CSV columns to DB columns
                            values = (
                                incident_id,
                                row.get('establishment_name', '').strip() or None,
                                row.get('city', '').strip() or None,
                                row.get('state', '').strip() or None,
                                naics_code or None,
                                row.get('industry_description', '').strip() or None,
                                safe_int(row.get('year_filing_for', '')),
                                row.get('date_of_incident', '').strip() or None,
                                safe_int(row.get('incident_outcome', '')),
                                safe_int_zero(row.get('dafw_num_away', '')),
                                safe_int_zero(row.get('djtr_num_tr', '')),
                                safe_int(row.get('type_of_incident', '')),
                                row.get('job_description', '').strip() or None,
                                # Narrative fields (mapped from NEW_NAR_* columns)
                                row.get('NEW_NAR_WHAT_HAPPENED', '').strip() or None,
                                row.get('NEW_NAR_BEFORE_INCIDENT', '').strip() or None,
                                row.get('NEW_INCIDENT_LOCATION', '').strip() or None,
                                row.get('NEW_NAR_INJURY_ILLNESS', '').strip() or None,
                                row.get('NEW_NAR_OBJECT_SUBSTANCE', '').strip() or None,
                                row.get('NEW_INCIDENT_DESCRIPTION', '').strip() or None,
                                # OIICS classification codes
                                row.get('nature_title_pred', '').strip() or None,
                                row.get('part_title_pred', '').strip() or None,
                                row.get('event_title_pred', '').strip() or None,
                                row.get('source_title_pred', '').strip() or None,
                                row.get('sec_source_title_pred', '').strip() or None,
                            )
                            
                            cursor.execute(insert_sql, values)
                            rows_in_file += 1
                            total_rows += 1
                    
                    break  # Successfully processed file, exit encoding loop
                    
            except (UnicodeDecodeError, UnicodeError) as e:
                # Try next encoding
                continue
        
        if not file_opened:
            print(f"  Error: Could not decode {filename} with any supported encoding")
            continue
        
        print(f"  Loaded {rows_in_file} rows from {filename}")
    
    conn.commit()
    return total_rows


def rebuild_fts_index(conn: sqlite3.Connection):
    """Rebuild the FTS5 index after data insertion."""
    cursor = conn.cursor()
    cursor.execute("INSERT INTO incidents_fts(incidents_fts) VALUES('rebuild')")
    conn.commit()


def print_statistics(conn: sqlite3.Connection):
    """Print statistics about loaded data."""
    cursor = conn.cursor()
    
    # Total rows
    cursor.execute("SELECT COUNT(*) FROM incidents")
    total = cursor.fetchone()[0]
    print(f"\n=== Statistics ===")
    print(f"Total rows loaded: {total:,}")
    
    # Deaths (incident_outcome = 1)
    cursor.execute("SELECT COUNT(*) FROM incidents WHERE incident_outcome = 1")
    deaths = cursor.fetchone()[0]
    print(f"Deaths (incident_outcome = 1): {deaths:,}")
    
    # Days away from work (incident_outcome = 2)
    cursor.execute("SELECT COUNT(*) FROM incidents WHERE incident_outcome = 2")
    dafw = cursor.fetchone()[0]
    print(f"Days away from work cases (incident_outcome = 2): {dafw:,}")
    
    # Top 5 most common event_title_pred values
    cursor.execute("""
        SELECT event_title_pred, COUNT(*) as cnt
        FROM incidents
        WHERE event_title_pred IS NOT NULL AND event_title_pred != ''
        GROUP BY event_title_pred
        ORDER BY cnt DESC
        LIMIT 5
    """)
    print(f"\nTop 5 most common event_title_pred values:")
    for event, count in cursor.fetchall():
        print(f"  {event}: {count:,}")
    
    # Sample 3 nar_what_happened values
    cursor.execute("""
        SELECT nar_what_happened
        FROM incidents
        WHERE nar_what_happened IS NOT NULL AND nar_what_happened != ''
        LIMIT 3
    """)
    print(f"\nSample nar_what_happened values:")
    for i, (nar,) in enumerate(cursor.fetchall(), 1):
        # Truncate if too long
        display = nar[:200] + "..." if len(nar) > 200 else nar
        print(f"  {i}. {display}")


def main(csv_files: Optional[List[str]] = None):
    """Main function to index OSHA CSV files."""
    if csv_files is None:
        csv_files = ['ITA_CaseDetail_2023.csv', 'ITA_CaseDetail_2024.csv']
    
    project_root = get_project_root()
    db_path = project_root / 'osha_incidents.db'
    
    # Delete existing database to start fresh
    if db_path.exists():
        print(f"Removing existing database at {db_path}...")
        db_path.unlink()
    
    print(f"Creating database at {db_path}...")
    conn = create_database(str(db_path))
    
    print(f"Loading CSV files...")
    total_rows = load_csv_files(conn, csv_files)
    
    print(f"\nRebuilding FTS5 index...")
    rebuild_fts_index(conn)
    
    print_statistics(conn)
    
    conn.close()
    print(f"\nIndexing complete! Database saved to {db_path}")


if __name__ == '__main__':
    main()
