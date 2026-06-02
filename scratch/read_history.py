from pathlib import Path
import json

def read_history():
    log_path = Path(r"C:\Users\rjkaj\.gemini\antigravity\brain\18873a38-e1bf-4914-86ba-444f343d0a91\.system_generated\logs\transcript.jsonl")
    if not log_path.exists():
        print("Transcript file not found")
        return
        
    lines = log_path.read_text(encoding="utf-8").splitlines()
    print(f"Total lines in transcript: {len(lines)}")
    
    # Print lines that contain user inputs
    for line in lines:
        try:
            data = json.loads(line)
            if data.get("type") == "USER_INPUT":
                print(f"User input: {data.get('content')}")
        except Exception as e:
            pass

if __name__ == "__main__":
    read_history()
