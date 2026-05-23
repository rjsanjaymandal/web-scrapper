import socket, sys

host = "lead-engine.railway.app"
ports = [5432, 5433, 5434, 6543, 7432, 8443, 443]

for port in ports:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        result = s.connect_ex((host, port))
        if result == 0:
            print(f"Port {port}: OPEN")
            # Try a quick PostgreSQL probe
            try:
                s.sendall(b"\x00\x00\x00\x08\x04\xd2\x16\x2f")
                data = s.recv(1024)
                print(f"  Response: {data[:100]}")
                if b"postgres" in data.lower() or b"pg" in data.lower() or b"SSL" in data:
                    print(f"  -> POSTGRESQL DETECTED on port {port}!")
            except:
                pass
        else:
            print(f"Port {port}: closed")
        s.close()
    except Exception as e:
        print(f"Port {port}: error {e}")
