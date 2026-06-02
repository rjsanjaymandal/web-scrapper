import re
from processing import ProcessingHandler

contact = {"name": "Test Name", "email": "400059emailsupport@schoolsuniverse.comcontact"}

# Step 0: Name cleaning
name = contact.get('name')
print("Step 0: name =", name)
name_str = re.sub(r'\s+', ' ', str(name)).strip()
name_str = re.sub(r'[\u00ae\u2122\u00a9]|(?:\([rR]\))|(?:\([tT][mM]\))|(?:\([cC]\))$', '', name_str).strip()
print("Step 0: name_str =", name_str)
junk_pattern = re.compile(r'\b(?:test|dummy|placeholder|unknown|no name|n/a|na)\b', re.I)
if len(name_str) < 3 or junk_pattern.search(name_str):
    print("Step 0: dropped because of junk_pattern or length!", junk_pattern.search(name_str))
else:
    contact['name'] = name_str
    
# Step 1: Phone
phone_clean = ProcessingHandler.normalize_phone(contact.get('phone'))
print("Step 1: phone_clean =", phone_clean)
contact['phone_clean'] = phone_clean
contact['phone'] = phone_clean

# Step 2: Email
email = str(contact.get('email') or '').strip().lower()
print("Step 2: email =", email)
cleaned_email = ProcessingHandler.clean_email_text(email)
print("Step 2: cleaned_email =", cleaned_email)
is_valid = ProcessingHandler.is_valid_email(cleaned_email)
print("Step 2: is_valid =", is_valid)

# Let's check step 6: Must have phone or email
has_phone = bool(contact.get('phone_clean'))
has_email = bool(cleaned_email and is_valid)
print("Step 6: has_phone =", has_phone, "has_email =", has_email)
