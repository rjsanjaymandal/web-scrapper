#!/usr/bin/env python3
"""
Quick Start: Financial Consultant Data Scraper
Run this directly to start scraping financial consultant data with default settings
"""
import os
import sys
import subprocess

def start_financial_scraping():
    """Start the financial consultant automation"""
    print("\n" + "="*80)
    print("Starting Financial Consultant Data Scraper")
    print("="*80)
    print("\nConfiguration:")
    print("  ✓ Target: All 108 Financial & Professional Consultant Categories")
    print("  ✓ Coverage: 135+ Indian Cities")
    print("  ✓ Sources: JustDial, TradeIndia, IndiaMART, Google Maps, Search Engines")
    print("  ✓ Deduplication: O(1) in-database de-duping")
    print("  ✓ Mode: Continuous infinite scraping with shuffled city/category rotation")
    print("\nStarting scraper...\n")
    
    automator_script = os.path.join(os.path.dirname(__file__), "automate_financial_consultants.py")
    
    try:
        subprocess.run([sys.executable, automator_script])
    except KeyboardInterrupt:
        print("\n\n" + "="*80)
        print("Scraper stopped by user")
        print("="*80)
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    start_financial_scraping()
