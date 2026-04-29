from scraper import load_config
import os

def test_config():
    print("--- Testing Config Loading ---")
    try:
        config = load_config()
        print(f"Successfully loaded config.")
        print(f"max_concurrent: {config.max_concurrent}")
        print(f"db_host: {config.db_host}")
        
        # Verify it can be accessed
        val = config.max_concurrent
        print("Verification: Accessing max_concurrent succeeded.")
        
    except AttributeError as e:
        print(f"FAILED: AttributeError still present: {e}")
    except Exception as e:
        print(f"FAILED: Unexpected error: {e}")

if __name__ == "__main__":
    test_config()
