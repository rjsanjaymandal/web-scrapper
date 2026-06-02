from pathlib import Path

def apply():
    dash_path = Path("dashboard.py")
    if not dash_path.exists():
        print("dashboard.py not found")
        return
        
    content = dash_path.read_text(encoding="utf-8")
    
    target = """                    if cleaned.get('phone') != row['phone'] or cleaned.get('email') != row['email']:
                        if USE_SQLITE:
                            cur.execute(
                                "UPDATE contacts SET phone = ?, phone_clean = ?, email = ?, email_valid = ? WHERE id = ?",
                                (cleaned.get('phone'), cleaned.get('phone_clean'), cleaned.get('email'), cleaned.get('email_valid'), contact_id)
                            )
                        else:
                            cur.execute(
                                "UPDATE contacts SET phone = %s, phone_clean = %s, email = %s, email_valid = %s WHERE id = %s",
                                (cleaned.get('phone'), cleaned.get('phone_clean'), cleaned.get('email'), cleaned.get('email_valid'), contact_id)
                            )
                        updated += 1"""

    replacement = """                    # Check if anything changed to save it all
                    if (cleaned.get('phone') != row['phone'] or 
                        cleaned.get('email') != row['email'] or 
                        cleaned.get('name') != row['name'] or 
                        cleaned.get('category') != row['category'] or
                        cleaned.get('quality_score') != row['quality_score'] or
                        cleaned.get('quality_tier') != row['quality_tier']):
                        
                        if USE_SQLITE:
                            cur.execute(
                                \"\"\"UPDATE contacts SET 
                                    name = ?, phone = ?, phone_clean = ?, email = ?, 
                                    email_valid = ?, category = ?, quality_score = ?, 
                                    quality_tier = ?, enriched = ? 
                                   WHERE id = ?\"\"\",
                                (cleaned.get('name'), cleaned.get('phone'), cleaned.get('phone_clean'), 
                                 cleaned.get('email'), cleaned.get('email_valid'), cleaned.get('category'), 
                                 cleaned.get('quality_score'), cleaned.get('quality_tier'), True, contact_id)
                            )
                        else:
                            cur.execute(
                                \"\"\"UPDATE contacts SET 
                                    name = %s, phone = %s, phone_clean = %s, email = %s, 
                                    email_valid = %s, category = %s, quality_score = %s, 
                                    quality_tier = %s, enriched = %s 
                                   WHERE id = %s\"\"\",
                                (cleaned.get('name'), cleaned.get('phone'), cleaned.get('phone_clean'), 
                                 cleaned.get('email'), cleaned.get('email_valid'), cleaned.get('category'), 
                                 cleaned.get('quality_score'), cleaned.get('quality_tier'), True, contact_id)
                            )
                        updated += 1"""

    # Invalidate cache block
    target_commit = """                conn.commit()
                cur.close()
                conn.close()
                set_status("Idle", False)"""
                
    replacement_commit = """                conn.commit()
                cur.close()
                conn.close()
                
                # Invalidate stats cache on DB modifications
                global STATS_CACHE
                STATS_CACHE.clear()
                
                set_status("Idle", False)"""

    target_norm = target.replace("\r\n", "\n")
    content_norm = content.replace("\r\n", "\n")
    
    if target_norm in content_norm:
        content_norm = content_norm.replace(target_norm, replacement.replace("\r\n", "\n"))
        
        target_commit_norm = target_commit.replace("\r\n", "\n")
        if target_commit_norm in content_norm:
            content_norm = content_norm.replace(target_commit_norm, replacement_commit.replace("\r\n", "\n"))
            
        if "\r\n" in content:
            content_norm = content_norm.replace("\n", "\r\n")
            
        dash_path.write_text(content_norm, encoding="utf-8")
        print("Success! Deep clean update fix and cache invalidation applied to dashboard.py.")
    else:
        print("Error: Target code block not found in dashboard.py!")

if __name__ == "__main__":
    apply()
