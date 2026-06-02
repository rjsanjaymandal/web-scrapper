from pathlib import Path

def apply():
    dash_path = Path("dashboard.py")
    if not dash_path.exists():
        print("dashboard.py not found")
        return
        
    content = dash_path.read_text(encoding="utf-8")
    
    target = """        # Stats reflect active filters (city, category, source, search, quality)
        if USE_SQLITE:
            cur.execute(f\"\"\"
                SELECT 
                    SUM(CASE WHEN phone_clean IS NOT NULL AND phone_clean <> '' THEN 1 ELSE 0 END) as with_phone,
                    SUM(CASE WHEN email IS NOT NULL AND email <> '' THEN 1 ELSE 0 END) as with_email,
                    COUNT(DISTINCT city) as city_count,
                    SUM(CASE WHEN LOWER(quality_tier) = 'high' THEN 1 ELSE 0 END) as q_high,
                    SUM(CASE WHEN LOWER(quality_tier) = 'medium' THEN 1 ELSE 0 END) as q_medium,
                    SUM(CASE WHEN LOWER(quality_tier) = 'low' THEN 1 ELSE 0 END) as q_low,
                    AVG(quality_score) as avg_score
                FROM contacts
                WHERE {where_sql}
            \"\"\", params)
        else:
            cur.execute(f\"\"\"
                SELECT 
                    COUNT(*) FILTER (WHERE phone_clean IS NOT NULL AND phone_clean <> '') as with_phone,
                    COUNT(*) FILTER (WHERE email IS NOT NULL AND email <> '') as with_email,
                    COUNT(DISTINCT city) as city_count,
                    COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'high') as q_high,
                    COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'medium') as q_medium,
                    COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'low') as q_low,
                    AVG(quality_score) as avg_score
                FROM contacts
                WHERE {where_sql}
            \"\"\", params)
        stats_row = cur.fetchone()
        if stats_row:
            stats_row = dict(stats_row)
        
        cur.execute(f"SELECT source, COUNT(*) as c FROM contacts WHERE {where_sql} GROUP BY source", params)
        by_source = {r["source"]: r["c"] for r in cur.fetchall()}
        cur.execute(f"SELECT category, COUNT(*) as c FROM contacts WHERE {where_sql} GROUP BY category", params)
        by_cat = {r["category"]: r["c"] for r in cur.fetchall()}
        cur.close()
        conn.close()"""

    replacement = """        # High-performance caching for stats and aggregations to prevent DB throttling
        cache_key = (where_sql, tuple(params))
        now = time.time()
        
        cached_data = None
        if cache_key in STATS_CACHE:
            val, ts = STATS_CACHE[cache_key]
            if (now - ts) < STATS_CACHE_TTL:
                cached_data = val
                
        if cached_data:
            stats_row = cached_data["stats_row"]
            by_source = cached_data["by_source"]
            by_cat = cached_data["by_cat"]
        else:
            # Stats reflect active filters (city, category, source, search, quality)
            if USE_SQLITE:
                cur.execute(f\"\"\"
                    SELECT 
                        SUM(CASE WHEN phone_clean IS NOT NULL AND phone_clean <> '' THEN 1 ELSE 0 END) as with_phone,
                        SUM(CASE WHEN email IS NOT NULL AND email <> '' THEN 1 ELSE 0 END) as with_email,
                        COUNT(DISTINCT city) as city_count,
                        SUM(CASE WHEN LOWER(quality_tier) = 'high' THEN 1 ELSE 0 END) as q_high,
                        SUM(CASE WHEN LOWER(quality_tier) = 'medium' THEN 1 ELSE 0 END) as q_medium,
                        SUM(CASE WHEN LOWER(quality_tier) = 'low' THEN 1 ELSE 0 END) as q_low,
                        AVG(quality_score) as avg_score
                    FROM contacts
                    WHERE {where_sql}
                \"\"\", params)
            else:
                cur.execute(f\"\"\"
                    SELECT 
                        COUNT(*) FILTER (WHERE phone_clean IS NOT NULL AND phone_clean <> '') as with_phone,
                        COUNT(*) FILTER (WHERE email IS NOT NULL AND email <> '') as with_email,
                        COUNT(DISTINCT city) as city_count,
                        COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'high') as q_high,
                        COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'medium') as q_medium,
                        COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'low') as q_low,
                        AVG(quality_score) as avg_score
                    FROM contacts
                    WHERE {where_sql}
                \"\"\", params)
            stats_row = cur.fetchone()
            if stats_row:
                stats_row = dict(stats_row)
            else:
                stats_row = {}
            
            cur.execute(f"SELECT source, COUNT(*) as c FROM contacts WHERE {where_sql} GROUP BY source", params)
            by_source = {r["source"]: r["c"] for r in cur.fetchall()}
            cur.execute(f"SELECT category, COUNT(*) as c FROM contacts WHERE {where_sql} GROUP BY category", params)
            by_cat = {r["category"]: r["c"] for r in cur.fetchall()}
            
            # Store in cache
            STATS_CACHE[cache_key] = ({
                "stats_row": stats_row,
                "by_source": by_source,
                "by_cat": by_cat
            }, now)
            
        cur.close()
        conn.close()"""

    if target in content:
        content = content.replace(target, replacement)
        dash_path.write_text(content, encoding="utf-8")
        print("Success! Caching code applied to dashboard.py.")
    else:
        # Let's try matching with universal newlines
        target_norm = target.replace("\r\n", "\n")
        content_norm = content.replace("\r\n", "\n")
        if target_norm in content_norm:
            content_norm = content_norm.replace(target_norm, replacement.replace("\r\n", "\n"))
            # Restore Windows line endings if the original had them
            if "\r\n" in content:
                content_norm = content_norm.replace("\n", "\r\n")
            dash_path.write_text(content_norm, encoding="utf-8")
            print("Success! Caching code applied with normalized newlines.")
        else:
            print("Error: Target code block not found in dashboard.py!")

if __name__ == "__main__":
    apply()
