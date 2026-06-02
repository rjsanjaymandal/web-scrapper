from processing import ProcessingHandler

contact = {"name": "Test Name", "email": "400059emailsupport@schoolsuniverse.comcontact"}
print("Initial contact:", contact)

res = ProcessingHandler.process_contact(contact)
print("Result contact:", res)
