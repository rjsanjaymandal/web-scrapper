from stealth_utils import DataImpulseManager

user = "1accade8fd4acb75b8ae"
city = "Delhi"
sid = "sess123"

print("--- DataImpulse Proxy Test ---")
basic = DataImpulseManager.format_auth(user, city, sid, enable_city=False)
print(f"EXPECTED (Basic): {user}__cr.in;sid.sess123;intvlv.300")
print(f"ACTUAL   (Basic): {basic}")

city_target = DataImpulseManager.format_auth(user, city, sid, enable_city=True)
print(f"EXPECTED (City):  {user}__ct.delhi;cr.in;sid.sess123;intvlv.300")
print(f"ACTUAL   (City):  {city_target}")
