"""
Utility script to query and view data from the fincs.db SQLite database.
"""
import sys
from pathlib import Path
from .storage import connect_db, get_all_trading_events, get_events_by_pair, get_latest_snapshot, get_event_statistics



def safe_print(text: str) -> None:
    """Print text safely on Windows consoles that may not support Unicode."""
    try:
        print(text)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "utf-8"
        print(text.encode(encoding, errors="backslashreplace").decode(encoding))


def print_separator(title: str = ""):
    """Print a separator line with optional title."""
    if title:
        print(f"\n{'=' * 80}")
        print(f"  {title}")
        print('=' * 80)
    else:
        print('-' * 80)


def show_statistics(db_path: str):
    """Display database statistics."""
    conn = connect_db(db_path)
    stats = get_event_statistics(conn)
    
    print_separator("DATABASE STATISTICS")
    print(f"\nTotal Events: {stats['total_events']}")
    print(f"Trading Events: {stats['trading_events']}")
    print(f"Non-Trading Events: {stats['non_trading_events']}")
    
    if stats['by_pair']:
        print("\nEvents by Currency Pair:")
        for pair, count in stats['by_pair'].items():
            print(f"  {pair}: {count}")
    
    if stats['by_action']:
        print("\nEvents by Action:")
        for action, count in stats['by_action'].items():
            print(f"  {action}: {count}")
    
    conn.close()


def show_latest_snapshot(db_path: str):
    """Display the latest raw snapshot."""
    conn = connect_db(db_path)
    snapshot = get_latest_snapshot(conn)
    
    print_separator("LATEST RAW SNAPSHOT")
    
    if snapshot:
        print(f"\nID: {snapshot['id']}")
        print(f"Scraped At: {snapshot['scraped_at']}")
        safe_print(f"Channel: {snapshot['channel']}")
        print(f"Hash: {snapshot['raw_hash'][:16]}...")
        print(f"Text Length: {len(snapshot['raw_text'])} characters")
        print(f"\nFirst 500 characters of raw text:")
        print_separator()
        safe_print(snapshot['raw_text'][:500])
        if len(snapshot['raw_text']) > 500:
            print("...")
    else:
        print("\nNo snapshots found in database.")
    
    conn.close()


def show_trading_events(db_path: str, limit: int = 20):
    """Display recent trading events."""
    conn = connect_db(db_path)
    events = get_all_trading_events(conn, limit=limit)
    
    print_separator(f"RECENT TRADING EVENTS (showing {min(limit, len(events))} of {len(events)})")
    
    if events:
        for event in events:
            print_separator()
            print(f"ID: {event['id']}")
            print(f"Scraped At: {event['scraped_at']}")
            print(f"Pair: {event['pair'] or 'N/A'}")
            print(f"Action: {event['action'] or 'N/A'}")
            print(f"Side: {event['side'] or 'N/A'}")
            print(f"Lot Ratio: {event['lot_ratio'] or 'N/A'}")
            print(f"Is Add: {'Yes' if event['is_add'] else 'No'}")
            safe_print(f"\nText:\n{event['segment_text']}")
    else:
        print("\nNo trading events found in database.")
    
    conn.close()


def show_events_by_pair(db_path: str, pair: str, limit: int = 20):
    """Display trading events for a specific currency pair."""
    conn = connect_db(db_path)
    events = get_events_by_pair(conn, pair, limit=limit)
    
    print_separator(f"TRADING EVENTS FOR {pair} (showing {min(limit, len(events))})")
    
    if events:
        for event in events:
            print_separator()
            print(f"ID: {event['id']}")
            print(f"Scraped At: {event['scraped_at']}")
            print(f"Action: {event['action'] or 'N/A'}")
            print(f"Side: {event['side'] or 'N/A'}")
            print(f"Lot Ratio: {event['lot_ratio'] or 'N/A'}")
            print(f"Is Add: {'Yes' if event['is_add'] else 'No'}")
            safe_print(f"\nText:\n{event['segment_text']}")
    else:
        print(f"\nNo trading events found for {pair}.")
    
    conn.close()


def export_to_csv(db_path: str, output_file: str = "trading_events.csv"):
    """Export trading events to CSV file."""
    import csv
    
    conn = connect_db(db_path)
    events = get_all_trading_events(conn, limit=999999)
    
    if not events:
        print("No trading events to export.")
        conn.close()
        return
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        # Get field names from the first event
        fieldnames = list(events[0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(events)
    
    print(f"Exported {len(events)} trading events to {output_file}")
    conn.close()


def main():
    """Main function with command-line interface."""
    db_path = "data/fincs.db"
    
    # Check if database exists
    if not Path(db_path).exists():
        print(f"Database not found at: {db_path}")
        print("Please run the scraper first to create the database.")
        return
    
    if len(sys.argv) < 2:
        print("Usage: python query_db.py [command] [options]")
        print("\nCommands:")
        print("  stats              - Show database statistics")
        print("  snapshot           - Show latest raw snapshot")
        print("  events [limit]     - Show recent trading events (default: 20)")
        print("  pair [PAIR] [limit] - Show events for specific pair (e.g., USDJPY)")
        print("  export [filename]  - Export all trading events to CSV")
        print("\nExamples:")
        print("  python query_db.py stats")
        print("  python query_db.py events 50")
        print("  python query_db.py pair USDJPY 30")
        print("  python query_db.py export my_trades.csv")
        return
    
    command = sys.argv[1].lower()
    
    if command == "stats":
        show_statistics(db_path)
    
    elif command == "snapshot":
        show_latest_snapshot(db_path)
    
    elif command == "events":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        show_trading_events(db_path, limit=limit)
    
    elif command == "pair":
        if len(sys.argv) < 3:
            print("Error: Please specify a currency pair (e.g., USDJPY)")
            return
        pair = sys.argv[2].upper()
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 20
        show_events_by_pair(db_path, pair, limit=limit)
    
    elif command == "export":
        output_file = sys.argv[2] if len(sys.argv) > 2 else "trading_events.csv"
        export_to_csv(db_path, output_file)
    
    else:
        print(f"Unknown command: {command}")
        print("Run without arguments to see available commands.")


if __name__ == "__main__":
    main()

