import sqlite3
import re
from pathlib import Path

def inspect_emails():
    db_path = Path("scraper_local.db")
    if not db_path.exists():
        print("Database not found")
        return
        
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id, name, email, phone, source, category FROM contacts WHERE email IS NOT NULL AND email <> ''")
    rows = cur.fetchall()
    
    print(f"Total contacts with email: {len(rows)}")
    print("\nListing all emails and checking for noise:")
    
    noise_suffixes = ['contact', 'support', 'phone', 'website', 'web', 'telephonenumber', 'address', 'mobile', 'home', 'about', 'office', 'fax', 'enquiry', 'enquiries', 'queries', 'query', 'email', 'go', 'view', 'map', 'maps', 'location', 'locations', 'details', 'detail', 'info', 'link', 'links', 'click', 'here', 'tel', 'call']
    
    corrupted_count = 0
    for row in rows:
        cid, name, email, phone, source, cat = row
        local_part, domain = email.split('@') if '@' in email else (email, '')
        
        has_noise = False
        reason = ""
        
        # Check suffix noise
        for suffix in noise_suffixes:
            if domain.lower().endswith(suffix) and not domain.lower().endswith(f".{suffix}"):
                # E.g. ends with comcontact, but not .contact
                # Let's see if the part before suffix is a valid TLD
                for tld in ['.com', '.in', '.org', '.net', '.co']:
                    if domain.lower().endswith(tld + suffix):
                        has_noise = True
                        reason = f"Domain ends with TLD+{suffix} ({domain})"
                        break
        
        # Check prefix noise
        if re.match(r'^\d+email', local_part.lower()) or re.match(r'^\d+contact', local_part.lower()) or re.match(r'^email', local_part.lower()) or re.match(r'^phone', local_part.lower()):
            has_noise = True
            reason = f"Local part has prefix noise ({local_part})"
            
        # Check character encoding corruption (\uFFFD)
        if '\uFFFD' in name or '\uFFFD' in email or '\uFFFD' in (phone or ''):
            has_noise = True
            reason = "Encoding corruption (\uFFFD)"
            
        if has_noise:
            corrupted_count += 1
            print(f"ID: {cid} | Name: {name} | Email: {email} | Phone: {phone} | Source: {source} | Reason: {reason}")
            
    print(f"\nTotal corrupted/noisy records detected: {corrupted_count}")
    conn.close()

if __name__ == "__main__":
    inspect_emails()
