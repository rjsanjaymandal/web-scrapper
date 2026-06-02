import os
import re
import sys
from bs4 import BeautifulSoup

def optimize_html(html_path):
    if not os.path.exists(html_path):
        print(f"Error: File '{html_path}' does not exist.")
        return False

    print(f"\n[+] Loading landing page layout: {html_path}")
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()

    soup = BeautifulSoup(content, 'html.parser')
    modified = False

    # 1. Optimize Heading Structure (Only one H1 tag allowed)
    h1s = soup.find_all('h1')
    if len(h1s) > 1:
        print(f"[!] Found {len(h1s)} H1 tags. Consolidating heading structure hierarchy...")
        # Keep the first one, or let the user choose. We keep the most valuable keyword H1 (the first index)
        for i, tag in enumerate(h1s[1:], start=2):
            original_text = tag.text.strip()
            # Convert to H2 tag
            tag.name = 'h2'
            # Add helper classes to keep original styles if needed
            existing_classes = tag.get('class', [])
            if 'h1-style' not in existing_classes:
                tag['class'] = existing_classes + ['h1-style']
            print(f"    -> Converted H1 #{i} to H2: '{original_text[:40]}...'")
        modified = True
    elif len(h1s) == 1:
        print("[+] Technical SEO check: Page contains exactly 1 H1 heading tag. (Passed)")
    else:
        print("[!] Technical SEO warning: Page is missing an H1 heading.")

    # 2. Defer Blocking Javascript (Inject defer into blocking scripts)
    scripts = soup.find_all('script')
    blocking_count = 0
    for script in scripts:
        if script.has_attr('src'):
            # If it's an external script and lacks async/defer, add defer
            if not script.has_attr('async') and not script.has_attr('defer'):
                script['defer'] = ''
                blocking_count += 1
    
    if blocking_count > 0:
        print(f"[!] Added 'defer' to {blocking_count} render-blocking external scripts.")
        modified = True
    else:
        print("[+] Performance check: All external script tags are deferred or asynchronous. (Passed)")

    # 3. Add font-display: swap to internal CSS stylesheet rules
    style_tags = soup.find_all('style')
    font_swaps_added = 0
    for style in style_tags:
        css_text = style.string
        if css_text and '@font-face' in css_text:
            # Parse @font-face and insert font-display: swap if missing
            updated_css = []
            blocks = re.split(r'(@font-face\s*\{[^}]*\})', css_text)
            for block in blocks:
                if block.startswith('@font-face'):
                    if 'font-display' not in block:
                        # Insert right before the closing bracket
                        block = block[:-1] + '    font-display: swap;\n}'
                        font_swaps_added += 1
                updated_css.append(block)
            
            if font_swaps_added > 0:
                style.string = "".join(updated_css)
                modified = True
                
    if font_swaps_added > 0:
        print(f"[!] Injected 'font-display: swap' inside {font_swaps_added} embedded font-face declarations.")

    # 4. Check for Accessibility Alt text
    images = soup.find_all('img')
    missing_alt = 0
    for img in images:
        if not img.has_attr('alt') or not img.get('alt').strip():
            img['alt'] = 'Maysan Labs Enterprise SaaS Services and Products'
            missing_alt += 1
            
    if missing_alt > 0:
        print(f"[!] Added descriptive alt attributes to {missing_alt} accessibility-deficient image elements.")
        modified = True

    # Save changes
    if modified:
        base, ext = os.path.splitext(html_path)
        out_path = f"{base}.optimized{ext}"
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))
        print(f"\n[SUCCESS] Optimization complete! Output saved to: {out_path}")
        print("[TIP] You can copy the code from this file directly into your landing page codebase.")
        return True
    else:
        print("\n[+] Landing page is already fully optimized under these rules.")
        return True

def optimize_css(css_path):
    if not os.path.exists(css_path):
        print(f"Error: File '{css_path}' does not exist.")
        return False

    print(f"\n[+] Loading stylesheet: {css_path}")
    with open(css_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Match @font-face blocks
    modified = False
    font_swaps_added = 0
    
    # Simple regex block parser to locate font-faces
    updated_css = []
    blocks = re.split(r'(@font-face\s*\{[^}]*\})', content)
    for block in blocks:
        if block.startswith('@font-face'):
            if 'font-display' not in block:
                # Insert font-display swap before closing bracket
                block = block.rstrip()[:-1] + '    font-display: swap;\n}'
                font_swaps_added += 1
                modified = True
        updated_css.append(block)

    if modified:
        base, ext = os.path.splitext(css_path)
        out_path = f"{base}.optimized{ext}"
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write("".join(updated_css))
        print(f"[SUCCESS] Font optimization complete! Injected {font_swaps_added} rules. Saved to: {out_path}")
        return True
    else:
        print("[+] Stylesheet fonts are already fully optimized. (Passed)")
        return True

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Maysan Labs Automated Website Optimization Engine")
        print("Usage:")
        print("  python optimize_landing_page.py <path_to_html_or_css>")
        sys.exit(1)
        
    target_path = sys.argv[1]
    ext = os.path.splitext(target_path)[1].lower()
    
    if ext in ['.html', '.htm']:
        optimize_html(target_path)
    elif ext == '.css':
        optimize_css(target_path)
    else:
        print(f"Error: Unsupported file format '{ext}'. Must be an .html or .css file.")
