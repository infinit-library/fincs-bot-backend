"""
Debug script to inspect the talk list page and find thread titles.
This helps troubleshoot issues with finding and clicking talk threads.
"""
import os
import time
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def visible(el) -> bool:
    try:
        return el.is_displayed()
    except Exception:
        return False


def main():
    """Inspect the talk list page to see what threads are available."""
    
    # Note: You need to manually navigate to the talk list page first
    # or copy the login logic from login_fincs.py
    
    print("=" * 80)
    print("TALK LIST PAGE INSPECTOR")
    print("=" * 80)
    print("\nThis script will inspect the talk list page and show all clickable elements.")
    print("You need to manually navigate to the talk list page after login.\n")
    
    load_dotenv()
    
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    wait = WebDriverWait(driver, 60)
    
    try:
        # Open the site
        driver.get("https://fincs.jp/")
        
        input("\nPlease login manually and navigate to the talk list page, then press ENTER...")
        
        print(f"\n[INFO] Current URL: {driver.current_url}")
        print(f"[INFO] Page Title: {driver.title}\n")
        
        # Find all clickable elements
        print("=" * 80)
        print("ALL CLICKABLE ELEMENTS (a, button, [role='button'])")
        print("=" * 80)
        
        all_clickables = driver.find_elements(By.CSS_SELECTOR, "a, button, [role='button']")
        print(f"\n[INFO] Found {len(all_clickables)} clickable elements\n")
        
        visible_count = 0
        for i, elem in enumerate(all_clickables):
            if not visible(elem):
                continue
            
            visible_count += 1
            text = (elem.text or "").strip()
            tag = elem.tag_name
            classes = elem.get_attribute("class") or ""
            href = elem.get_attribute("href") or ""
            
            if text or href:
                print(f"[{visible_count}] {tag.upper()}")
                if text:
                    print(f"    Text: {text[:100]}")
                if classes:
                    print(f"    Classes: {classes[:100]}")
                if href:
                    print(f"    Href: {href[:100]}")
                print()
        
        print("=" * 80)
        print(f"[INFO] Total visible clickable elements: {visible_count}")
        print("=" * 80)
        
        # Look for specific keywords
        print("\n" + "=" * 80)
        print("ELEMENTS CONTAINING 'エントリー' OR '決済' OR 'タイミング'")
        print("=" * 80 + "\n")
        
        keywords = ["エントリー", "決済", "タイミング"]
        found_count = 0
        
        for elem in all_clickables:
            if not visible(elem):
                continue
            
            text = (elem.text or "").strip()
            if any(kw in text for kw in keywords):
                found_count += 1
                print(f"[Match {found_count}] {elem.tag_name.upper()}")
                print(f"    Text: {text}")
                print(f"    Classes: {elem.get_attribute('class') or 'None'}")
                print()
        
        print("=" * 80)
        print(f"[INFO] Found {found_count} elements matching keywords")
        print("=" * 80)
        
        # Check for div elements with text content
        print("\n" + "=" * 80)
        print("ALL DIV ELEMENTS WITH TEXT CONTENT")
        print("=" * 80 + "\n")
        
        all_divs = driver.find_elements(By.CSS_SELECTOR, "div")
        div_count = 0
        
        for div in all_divs[:100]:  # Limit to first 100 visible divs
            if not visible(div):
                continue
            
            text = (div.text or "").strip()
            if text and len(text) < 200 and any(kw in text for kw in keywords):
                div_count += 1
                print(f"[Div {div_count}]")
                print(f"    Text: {text}")
                print(f"    Classes: {div.get_attribute('class') or 'None'}")
                print()
        
        print("=" * 80)
        print(f"[INFO] Found {div_count} divs matching keywords")
        print("=" * 80)
        
        # Try JavaScript inspection
        print("\n" + "=" * 80)
        print("JAVASCRIPT INSPECTION - DOCUMENT TEXT CONTENT")
        print("=" * 80 + "\n")
        
        body_text = driver.execute_script("return document.body.innerText;")
        if "エントリー" in body_text and "決済" in body_text and "タイミング" in body_text:
            print("[INFO] ✓ All keywords found in page text!")
            
            # Find the context around the keywords
            lines = body_text.split('\n')
            for i, line in enumerate(lines):
                if "エントリー" in line and "決済" in line:
                    print(f"\n[Context around line {i}]")
                    start = max(0, i - 2)
                    end = min(len(lines), i + 3)
                    for j in range(start, end):
                        marker = " >>> " if j == i else "     "
                        print(f"{marker}{lines[j]}")
        else:
            print("[WARNING] Not all keywords found in page text")
            if "エントリー" in body_text:
                print("  ✓ Found: エントリー")
            if "決済" in body_text:
                print("  ✓ Found: 決済")
            if "タイミング" in body_text:
                print("  ✓ Found: タイミング")
        
        print("\n" + "=" * 80)
        
        input("\n\nPress ENTER to close the browser...")
        
    finally:
        driver.quit()


if __name__ == "__main__":
    main()

