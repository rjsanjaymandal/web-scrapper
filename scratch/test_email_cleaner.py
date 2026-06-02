import re

def clean_email(email: str) -> str:
    if not email:
        return ""
    email = email.strip().lower()
    
    # 1. Split local part and domain
    if '@' not in email:
        return email
    local_part, domain = email.split('@', 1)
    
    # 2. Suffix noise words list
    noise_suffixes = [
        'contact', 'contacts', 'support', 'phone', 'website', 'web', 
        'telephonenumber', 'address', 'mobile', 'home', 'about', 
        'office', 'fax', 'enquiry', 'enquiries', 'queries', 'query', 
        'email', 'go', 'view', 'map', 'maps', 'location', 'locations', 
        'details', 'detail', 'info', 'link', 'links', 'click', 'here', 
        'tel', 'call'
    ]
    
    # Check if domain ends with TLD + noise_suffix
    # Sort TLDs by length descending to match longer ones first (.co.in before .in)
    tlds = ['.co.in', '.com.in', '.net.in', '.org.in', '.gov.in', '.ac.in', '.com', '.in', '.org', '.net', '.co', '.edu', '.gov', '.info', '.biz']
    
    domain_cleaned = domain
    for suffix in noise_suffixes:
        for tld in tlds:
            # We match case-insensitively, e.g. domain ending with .comcontact or .inGo
            pattern = re.escape(tld) + re.escape(suffix) + r'$'
            if re.search(pattern, domain_cleaned, re.IGNORECASE):
                # Replace TLD + suffix with just TLD
                domain_cleaned = re.sub(pattern, tld, domain_cleaned, flags=re.IGNORECASE)
                print(f"  [CLEAN SUFFIX] {domain} -> {domain_cleaned} (removed {suffix})")
                break
                
    # 3. Prefix noise
    # Pattern: starting with optional digits, then "email" or "contact" or "phone" or "tel"
    # e.g., "400059emailsupport" -> "support"
    # e.g., "110001contactprincipal" -> "principal"
    # e.g., "emailsupport" -> "support"
    local_cleaned = local_part
    prefix_patterns = [
        r'^\d*email',
        r'^\d*contact',
        r'^\d*phone',
        r'^\d*tel',
        r'^\d+_' # e.g. 123456_support
    ]
    
    for pattern in prefix_patterns:
        match = re.match(pattern, local_cleaned, re.IGNORECASE)
        if match:
            # Strip the matched prefix
            matched_len = match.end()
            candidate = local_cleaned[matched_len:]
            # Ensure the remaining part is not empty and is a plausible local part
            if len(candidate) >= 3:
                # Strip leading non-alphanumeric chars like dots, dashes, underscores
                candidate = re.sub(r'^[^a-zA-Z0-9]+', '', candidate)
                if len(candidate) >= 3:
                    print(f"  [CLEAN PREFIX] {local_part} -> {candidate} (removed prefix)")
                    local_cleaned = candidate
                    break
                    
    return f"{local_cleaned}@{domain_cleaned}"

# Test cases
test_emails = [
    "400059emailsupport@schoolsuniverse.comcontact",
    "dvsekhar@sebi.gov.inGo",
    "principal.ncsd@gmail.com",
    "110001contactprincipal@school.educontact",
    "info@schooldekho.org",
    "emailadmin@mysite.com",
    "99999_admin@test.co.insupport"
]

print("Running email cleaner tests:")
for em in test_emails:
    print(f"Original: {em}")
    res = clean_email(em)
    print(f"Cleaned:  {res}\n")
