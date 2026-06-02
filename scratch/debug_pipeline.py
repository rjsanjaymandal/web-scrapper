from processing import ProcessingHandler

test_cases_email = [
    "400059emailsupport@schoolsuniverse.comcontact",
    "dvsekhar@sebi.gov.inGo",
    "110001contactprincipal@school.educontact",
    "99999_admin@test.co.insupport",
    "info@schooldekho.org"
]

print("Debugging email cleaner in ProcessingHandler:")
for em in test_cases_email:
    cleaned = ProcessingHandler.clean_email_text(em)
    valid = ProcessingHandler.is_valid_email(cleaned)
    print(f"Original: {em}")
    print(f"  Cleaned: {cleaned}")
    print(f"  Valid:   {valid}")
