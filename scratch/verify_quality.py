from processing import ProcessingHandler
import sys

def verify():
    print("Executing Data Quality Pipeline Assertions:")
    
    # 1. Test Trademark Name Trimming
    test_cases_name = {
        "School Dekho®": "School Dekho",
        "Maysan Labs™": "Maysan Labs",
        "Ahlcon International School (R)": "Ahlcon International School",
        "Amity International School(TM)": "Amity International School",
        "Dav Public School (C)": "Dav Public School",
        "Just Dial Limited": "Just Dial Limited"
    }
    
    name_errors = 0
    for original, expected in test_cases_name.items():
        contact = {"name": original, "phone": "9999999999"} # must have valid phone/email to not be dropped
        res = ProcessingHandler.process_contact(contact)
        if not res:
            print(f"  [FAIL] {original} -> dropped entirely!")
            name_errors += 1
            continue
        cleaned = res["name"]
        if cleaned == expected:
            print(f"  [PASS] Name: '{original}' -> '{cleaned}'")
        else:
            print(f"  [FAIL] Name: '{original}' expected '{expected}', got '{cleaned}'")
            name_errors += 1

    # 2. Test Email Suffix and Prefix Cleaning & Recovery
    test_cases_email = {
        "400059emailsupport@schoolsuniverse.comcontact": "support@schoolsuniverse.com",
        "dvsekhar@sebi.gov.inGo": "dvsekhar@sebi.gov.in",
        "110001contactprincipal@school.educontact": "principal@school.edu",
        "99999_admin@test.co.insupport": "admin@test.co.in",
        "info@schooldekho.org": "info@schooldekho.org"
    }
    
    email_errors = 0
    for original, expected in test_cases_email.items():
        contact = {"name": "John Doe", "email": original}
        res = ProcessingHandler.process_contact(contact)
        if not res:
            print(f"  [FAIL] {original} -> dropped entirely!")
            email_errors += 1
            continue
        cleaned = res["email"]
        if cleaned == expected:
            print(f"  [PASS] Email: '{original}' -> '{cleaned}'")
        else:
            print(f"  [FAIL] Email: '{original}' expected '{expected}', got '{cleaned}'")
            email_errors += 1

    print("\nSummary:")
    print(f"Name cleaning errors: {name_errors}")
    print(f"Email cleaning errors: {email_errors}")
    
    if name_errors == 0 and email_errors == 0:
        print("\nAll pipeline assertions passed perfectly!")
        sys.exit(0)
    else:
        print("\nSome pipeline assertions failed.")
        sys.exit(1)

if __name__ == "__main__":
    verify()
