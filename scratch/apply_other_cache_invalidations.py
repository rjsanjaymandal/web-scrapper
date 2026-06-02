from pathlib import Path

def apply():
    dash_path = Path("dashboard.py")
    if not dash_path.exists():
        print("dashboard.py not found")
        return
        
    content = dash_path.read_text(encoding="utf-8")
    
    # 1. /api/cleanup/empty
    target_empty = """        deleted_count = cur.rowcount
        conn.commit()
        cur.execute("SELECT COUNT(*) as cnt FROM contacts")"""
        
    replacement_empty = """        deleted_count = cur.rowcount
        conn.commit()
        
        # Clear stats cache on DB modifications
        global STATS_CACHE
        STATS_CACHE.clear()
        
        cur.execute("SELECT COUNT(*) as cnt FROM contacts")"""

    # 2. /api/cleanup/quality
    target_quality = """        conn.commit()
        cur.close()
        conn.close()
        set_status("Idle", False)
        return jsonify({"success": True, "updated": updated})"""
        
    replacement_quality = """        conn.commit()
        cur.close()
        conn.close()
        
        # Clear stats cache on DB modifications
        global STATS_CACHE
        STATS_CACHE.clear()
        
        set_status("Idle", False)
        return jsonify({"success": True, "updated": updated})"""

    # 3. /api/maintenance/normalize
    target_normalize = """        conn = get_db()
        stats = ProcessingHandler.clean_database_logic(conn)
        conn.close()
        
        set_status("Idle", False)"""
        
    replacement_normalize = """        conn = get_db()
        stats = ProcessingHandler.clean_database_logic(conn)
        conn.close()
        
        # Clear stats cache on DB modifications
        global STATS_CACHE
        STATS_CACHE.clear()
        
        set_status("Idle", False)"""

    target_empty_norm = target_empty.replace("\r\n", "\n")
    content_norm = content.replace("\r\n", "\n")
    
    if target_empty_norm in content_norm:
        content_norm = content_norm.replace(target_empty_norm, replacement_empty.replace("\r\n", "\n"))
        
        target_quality_norm = target_quality.replace("\r\n", "\n")
        if target_quality_norm in content_norm:
            content_norm = content_norm.replace(target_quality_norm, replacement_quality.replace("\r\n", "\n"))
            
        target_normalize_norm = target_normalize.replace("\r\n", "\n")
        if target_normalize_norm in content_norm:
            content_norm = content_norm.replace(target_normalize_norm, replacement_normalize.replace("\r\n", "\n"))
            
        if "\r\n" in content:
            content_norm = content_norm.replace("\n", "\r\n")
            
        dash_path.write_text(content_norm, encoding="utf-8")
        print("Success! Additional cache invalidations applied to dashboard.py.")
    else:
        print("Error: Target empty/quality/normalize blocks not found in dashboard.py!")

if __name__ == "__main__":
    apply()
