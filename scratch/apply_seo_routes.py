from pathlib import Path

def apply():
    dash_path = Path("dashboard.py")
    if not dash_path.exists():
        print("dashboard.py not found")
        return
        
    content = dash_path.read_text(encoding="utf-8")
    
    target = 'if __name__ == "__main__":'
    
    routes_code = """@app.route("/seo-checker")
def seo_checker():
    config = load_config()
    return render_template_string(
        HTML,
        is_seo_checker=True,
        is_school_dashboard=False,
        contacts=[],
        s={"total": 0, "phone": 0, "email": 0, "cities": 0, "filtered_total": 0, "quality_high": 0, "quality_medium": 0, "quality_low": 0, "avg_quality": 0, "with_phone_pct": 0, "with_email_pct": 0},
        by_source={},
        by_cat={},
        page=1,
        total_pages=1,
        cities_default=config.get("cities", []),
        categories_default=config.get("categories", []),
        cities=[],
        categories=[],
        sources=[],
        selected_city="",
        selected_category="",
        selected_source="",
        selected_quality="",
        search_query="",
        sort_by="date",
        limit=50
    )


@app.route("/api/seo-check")
async def api_seo_check():
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"success": False, "error": "URL parameter is required"}), 400
        
    try:
        from seo_analyzer import SEOAnalyzer
        html, load_time, error = await SEOAnalyzer.fetch_html(url)
        if error:
            return jsonify({"success": False, "error": error}), 500
            
        report = SEOAnalyzer.analyze(url, html, load_time)
        return jsonify({"success": True, "report": report})
    except Exception as e:
        logger.error(f"SEO check failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


"""

    if target in content:
        content = content.replace(target, routes_code + target)
        dash_path.write_text(content, encoding="utf-8")
        print("Success! SEO checker routes appended to dashboard.py.")
    else:
        print("Error: Target 'if __name__ == __main__:' not found in dashboard.py!")

if __name__ == "__main__":
    apply()
