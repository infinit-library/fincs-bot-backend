import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException
from webdriver_manager.chrome import ChromeDriverManager

from .process_content import save_snapshot_and_segments

def js_click(driver, el):
    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center'});", el
    )
    driver.execute_script("arguments[0].click();", el)


def visible(el) -> bool:
    try:
        return el.is_displayed()
    except Exception:
        return False


def click_continue_with_email(driver, wait):
    """
    Click the auth provider button for
    'メールアドレスで続ける / ログイン'
    (NOT Google / Apple).
    """
    candidates = wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, ".v-btn.v-btn--block.bg-white")
        )
    )

    for el in candidates:
        if not visible(el):
            continue

        txt = (
            driver.execute_script(
                "return (arguments[0].innerText || '').trim();", el
            )
            or ""
        ).strip()

        if "メール" in txt and ("続ける" in txt or "ログイン" in txt):
            js_click(driver, el)
            time.sleep(1.5)
            return

    raise RuntimeError("Could not find メールアドレスで続ける / ログイン button.")


def find_best_scroll_container(driver):
    """
    Talk page uses a scrollable container (not always the window).
    Pick the element with the largest scrollable area among scrollable nodes.
    Returns a DOM element reference usable as a WebElement in execute_script.
    """
    return driver.execute_script(
        """
        const isScrollable = (el) => {
          if (!el) return false;
          const style = window.getComputedStyle(el);
          const oy = style.overflowY;
          if (oy !== 'auto' && oy !== 'scroll') return false;
          return (el.scrollHeight - el.clientHeight) > 200;
        };

        let best = document.scrollingElement || document.documentElement;
        let bestDelta = (best.scrollHeight - best.clientHeight) || 0;

        // Prefer common containers first
        const preferred = Array.from(document.querySelectorAll(
          '.vue-recycle-scroller, .vue-recycle-scroller__item-wrapper, main, section, div'
        ));

        for (const el of preferred) {
          if (!isScrollable(el)) continue;
          const delta = el.scrollHeight - el.clientHeight;
          if (delta > bestDelta) {
            best = el;
            bestDelta = delta;
          }
        }
        return best;
        """
    )


def find_scroll_container_from_messages(driver):
    """
    Prefer the scroll container that actually owns the message list by starting
    from the first div.content.isText and walking up to:
      - .vue-recycle-scroller (virtual scroller root)
      - otherwise: nearest overflow-y scroll/auto ancestor
    Falls back to find_best_scroll_container().
    """
    try:
        return driver.execute_script(
            """
            const msg = document.querySelector('div.content.isText');
            if (!msg) return null;

            const isScrollable = (el) => {
              if (!el) return false;
              const style = window.getComputedStyle(el);
              const oy = style.overflowY;
              if (oy !== 'auto' && oy !== 'scroll') return false;
              return (el.scrollHeight - el.clientHeight) > 50;
            };

            // Prefer virtual scroller root if present
            let n = msg;
            while (n && n !== document.body) {
              if (n.classList && n.classList.contains('vue-recycle-scroller')) return n;
              n = n.parentElement;
            }

            // Otherwise nearest scrollable ancestor
            n = msg;
            while (n && n !== document.body) {
              if (isScrollable(n)) return n;
              n = n.parentElement;
            }

            return null;
            """
        )
    except Exception:
        return None


def collect_all_istext_contents(driver, max_scrolls: int = 2500, pause_s: float = 0.35):
    """
    Collect all message texts from div.content.isText while scrolling downward.
    The talk page list is virtualized, so we must collect as we scroll.

    Returns: list[str] ordered by data-index when available.
    """
    scroll_el = find_scroll_container_from_messages(driver) or find_best_scroll_container(driver)

    def closest_attr(el, attr_name: str):
        try:
            return driver.execute_script(
                """
                let n = arguments[0];
                const attr = arguments[1];
                while (n && n.getAttribute && !n.getAttribute(attr)) n = n.parentElement;
                return n && n.getAttribute ? n.getAttribute(attr) : null;
                """,
                el,
                attr_name,
            )
        except Exception:
            return None

    # index -> text (keep latest seen; indices are unique per message)
    by_index = {}
    # fallback stable unique keys for items without index
    extras_seen = set()
    extras = []

    def collect_visible_once():
        els = driver.find_elements(By.CSS_SELECTOR, "div.content.isText")
        for el in els:
            if not visible(el):
                continue
            txt = (el.text or "").strip()
            if not txt:
                continue

            idx = closest_attr(el, "data-index")
            if idx is not None and str(idx).isdigit():
                by_index[int(idx)] = txt
            else:
                talkid = closest_attr(el, "data-talkid")
                key = (str(idx) if idx is not None else "") + "|" + (str(talkid) if talkid else "") + "|" + txt
                if key not in extras_seen:
                    extras_seen.add(key)
                    extras.append(txt)

    def get_metrics():
        try:
            top = driver.execute_script("return arguments[0].scrollTop;", scroll_el)
            height = driver.execute_script("return arguments[0].scrollHeight;", scroll_el)
            client = driver.execute_script("return arguments[0].clientHeight;", scroll_el)
            return float(top), float(height), float(client)
        except Exception:
            top = driver.execute_script("return window.scrollY;")
            height = driver.execute_script("return document.body.scrollHeight;")
            client = driver.execute_script("return window.innerHeight;")
            return float(top), float(height), float(client)

    def scroll_by(delta: float):
        try:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[1];", scroll_el, delta)
        except Exception:
            driver.execute_script("window.scrollBy(0, arguments[0]);", delta)

    def scroll_to(pos: float):
        try:
            driver.execute_script("arguments[0].scrollTop = arguments[1];", scroll_el, pos)
        except Exception:
            driver.execute_script("window.scrollTo(0, arguments[0]);", pos)

    # 1) From current position, scroll UP until no more older content loads
    stagnant_rounds = 0
    last_top = None
    for _ in range(max_scrolls):
        before = len(by_index) + len(extras)
        collect_visible_once()

        top, height, client = get_metrics()
        scroll_by(-client * 0.9)
        time.sleep(pause_s)

        top_after, height_after, client_after = get_metrics()
        after = len(by_index) + len(extras)

        if last_top is not None and top_after == last_top and after == before:
            stagnant_rounds += 1
        elif after == before:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0

        last_top = top_after

        # Near top and nothing new => likely reached oldest
        if top_after <= 1 and after == before:
            stagnant_rounds += 3

        if stagnant_rounds >= 12:
            break

    # 2) Jump to TOP explicitly, then scroll DOWN to the end collecting everything
    scroll_to(0)
    time.sleep(pause_s)

    stagnant_rounds = 0
    last_top = None
    for _ in range(max_scrolls):
        before = len(by_index) + len(extras)
        collect_visible_once()

        top, height, client = get_metrics()
        scroll_by(client * 0.9)
        time.sleep(pause_s)

        top_after, height_after, client_after = get_metrics()
        after = len(by_index) + len(extras)

        if last_top is not None and top_after == last_top and after == before:
            stagnant_rounds += 1
        elif after == before:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0

        last_top = top_after

        # Near bottom and nothing new => likely reached newest
        if (height_after - (top_after + client_after)) < 5 and after == before:
            stagnant_rounds += 3

        if stagnant_rounds >= 12:
            break

    ordered = [by_index[k] for k in sorted(by_index.keys())]
    return ordered + extras


def open_talk_thread_by_title(driver, wait, title: str):
    """
    Open a specific talk thread by visible title text, avoiding clicks on message bubbles.
    If already on the thread (title appears in header), this is a no-op.
    """
    print(f"\n[DEBUG] Looking for talk thread: {title}")
    
    # Handle '&' vs '＆' differences by matching keywords
    keywords = [k.strip() for k in re.split(r"[&＆]", title) if k.strip()]
    if not keywords:
        keywords = [title]
    
    print(f"[DEBUG] Keywords: {keywords}")

    # XPath that matches the title even if the ampersand differs (by requiring all keywords)
    kw_pred = " and ".join([f"contains(normalize-space(.),'{k}')" for k in keywords])
    title_anywhere_xpath = f"//*[{kw_pred}]"

    def on_thread() -> bool:
        try:
            hits = driver.find_elements(
                By.XPATH,
                f"//header//*[{kw_pred}] | "
                f"//*[contains(@class,'header') or contains(@class,'title') or contains(@class,'talk')][{kw_pred}] | "
                f"//main//*[self::h1 or self::h2][{kw_pred}]",
            )
            return any(visible(x) for x in hits)
        except Exception:
            return False

    if on_thread():
        print("[DEBUG] Already on the thread page")
        return

    # Try direct click strategies before scrolling
    print("[DEBUG] Trying direct click strategies...")
    
    # Strategy 1: Look for clickable items (a, button, div with role) that contain all keywords
    try:
        clickable_xpath = (
            f"//*[self::a or self::button or self::div[@role='button'] or self::li]"
            f"[{kw_pred}]"
        )
        direct_candidates = driver.find_elements(By.XPATH, clickable_xpath)
        print(f"[DEBUG] Found {len(direct_candidates)} direct clickable candidates")
        for c in direct_candidates:
            if visible(c):
                try:
                    print(f"[DEBUG] Trying to click: {c.text[:50]}...")
                    js_click(driver, c)
                    WebDriverWait(driver, 10).until(lambda d: on_thread())
                    print("[DEBUG] Successfully opened thread!")
                    return
                except Exception as e:
                    print(f"[DEBUG] Click failed: {e}")
                    continue
    except Exception as e:
        print(f"[DEBUG] Direct click strategy failed: {e}")

    # Strategy 2: Look for links/buttons with href or @click containing thread info
    try:
        all_links = driver.find_elements(By.CSS_SELECTOR, "a, button, [role='button']")
        print(f"[DEBUG] Checking {len(all_links)} links/buttons for keyword matches...")
        for link in all_links:
            if not visible(link):
                continue
            link_text = (link.text or "").strip()
            # Check if all keywords are in the text
            if all(kw in link_text for kw in keywords):
                try:
                    print(f"[DEBUG] Found matching link: {link_text[:50]}...")
                    js_click(driver, link)
                    WebDriverWait(driver, 10).until(lambda d: on_thread())
                    print("[DEBUG] Successfully opened thread!")
                    return
                except Exception as e:
                    print(f"[DEBUG] Click failed: {e}")
                    continue
    except Exception as e:
        print(f"[DEBUG] Link iteration strategy failed: {e}")

    # Strategy 3: Scroll and search (original method with improvements)
    print("[DEBUG] Starting scroll and search strategy...")
    scroll_el = None
    try:
        scroll_el = find_best_scroll_container(driver)
        print(f"[DEBUG] Scroll container found: {scroll_el is not None}")
    except Exception:
        scroll_el = None

    def scroll_list_down():
        nonlocal scroll_el
        if scroll_el is None:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
            return
        try:
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight * 0.9;",
                scroll_el,
            )
        except Exception:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)

    def scroll_list_to_top():
        nonlocal scroll_el
        if scroll_el is None:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.HOME)
            return
        try:
            driver.execute_script("arguments[0].scrollTop = 0;", scroll_el)
        except Exception:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.HOME)

    scroll_list_to_top()
    time.sleep(0.6)

    stagnant = 0
    last_top = None
    attempts = 0

    for iteration in range(350):
        # Try to find visible title nodes and click the nearest clickable ancestor
        candidates = driver.find_elements(By.XPATH, title_anywhere_xpath)
        
        if iteration % 10 == 0:
            print(f"[DEBUG] Iteration {iteration}: Found {len(candidates)} candidates")
        
        for c in candidates:
            if not visible(c):
                continue
            try:
                attempts += 1
                clickable = c
                parents = c.find_elements(
                    By.XPATH,
                    "./ancestor-or-self::*[self::a or self::button or @role='button' or @onclick][1]",
                )
                if parents:
                    clickable = parents[0]
                
                print(f"[DEBUG] Attempt {attempts}: Clicking candidate: {c.text[:50]}...")
                js_click(driver, clickable)
                WebDriverWait(driver, 10).until(lambda d: on_thread())
                print("[DEBUG] Successfully opened thread!")
                return
            except Exception as e:
                if iteration % 10 == 0:
                    print(f"[DEBUG] Click attempt failed: {e}")
                continue

        # Scroll and detect stagnation/bottom
        try:
            top = driver.execute_script("return arguments[0].scrollTop;", scroll_el) if scroll_el is not None else None
        except Exception:
            top = None

        scroll_list_down()
        time.sleep(0.35)

        try:
            top_after = driver.execute_script("return arguments[0].scrollTop;", scroll_el) if scroll_el is not None else None
        except Exception:
            top_after = None

        if top_after is not None and last_top is not None and top_after == last_top:
            stagnant += 1
        elif top_after is None and top is not None and last_top is not None and top == last_top:
            stagnant += 1
        else:
            stagnant = 0
        last_top = top_after if top_after is not None else top

        if stagnant >= 12:
            print(f"[DEBUG] Reached bottom/stagnant after {iteration} iterations")
            break

    # Last resort: print all visible text to help debug
    print("\n[DEBUG] Failed to find thread. Dumping visible clickable elements with partial matches:")
    try:
        all_clickables = driver.find_elements(By.CSS_SELECTOR, "a, button, [role='button']")
        for elem in all_clickables[:50]:  # Limit to first 50
            if visible(elem):
                text = (elem.text or "").strip()
                if text and any(kw in text for kw in keywords):
                    print(f"  - {text[:100]}")
    except Exception:
        pass

    raise RuntimeError(f"Could not open talk thread by title: {title}")


def main(auto_exit: bool = False):
    # ---- Load credentials ----
    load_dotenv()
    EMAIL = os.getenv("FINCS_EMAIL")
    PASSWORD = os.getenv("FINCS_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise RuntimeError("FINCS_EMAIL / FINCS_PASSWORD not found in .env")

    # ---- Browser setup ----
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    if os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes"):
        options.add_argument("--headless=new")  # headless for scheduler

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    wait = WebDriverWait(driver, 60)

    try:
        # 1) Open fincs.jp
        driver.get("https://fincs.jp/")
        wait.until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(1)

        # 2) Open auth modal (try header "登録/ログイン", but don't hard-fail if UI changed)
        def open_login_entrypoint():
            # Strategy A: click any visible button/link with 登録 or ログイン text
            try:
                candidates = driver.find_elements(
                    By.XPATH,
                    "//*[self::a or self::button or @role='button']"
                    "[contains(normalize-space(.),'ログイン') or contains(normalize-space(.),'登録')]",
                )
                for el in candidates:
                    if visible(el):
                        js_click(driver, el)
                        time.sleep(1.5)
                        return True
            except Exception:
                pass

            # Strategy B: old selector (label span) but click a clickable ancestor like open_fincs.py
            try:
                labels = driver.find_elements(By.CSS_SELECTOR, ".title-text.text-truncate")
                for label in labels:
                    if not visible(label):
                        continue
                    if ("ログイン" not in (label.text or "")) and ("登録" not in (label.text or "")):
                        continue
                    clickable = None
                    xpath_clickable = (
                        "./ancestor-or-self::*[self::a or self::button or @role='button' or @onclick][1]"
                    )
                    parents = label.find_elements(By.XPATH, xpath_clickable)
                    if parents:
                        clickable = parents[0]
                    else:
                        fallback = label.find_elements(
                            By.XPATH, "./ancestor-or-self::div[1] | ./ancestor-or-self::li[1]"
                        )
                        if fallback:
                            clickable = fallback[0]
                    if clickable:
                        js_click(driver, clickable)
                        time.sleep(1.5)
                        return True
            except Exception:
                pass

            # Strategy C: direct login URL fallback (site sometimes routes auth here)
            try:
                driver.get("https://fincs.jp/login")
                wait.until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                time.sleep(1.0)
                return True
            except Exception:
                return False

        open_login_entrypoint()

        # 3) Click "メールアドレスで続ける"
        click_continue_with_email(driver, wait)

        # =========================================================
        # 4) LOGIN FORM (VUETIFY-SAFE, PLACEHOLDER-BASED)
        # =========================================================

        # Don't assume Vuetify renders a visible v-dialog/v-overlay here (it may route to a page).
        # Instead, wait for the actual login inputs to exist.
        email_input_xpath = "//input[@type='text' and contains(@placeholder,'メールアドレス')]"
        password_input_xpath = "//input[@type='password' and contains(@placeholder,'パスワード')]"
        wait.until(
            lambda d: d.find_elements(By.XPATH, email_input_xpath)
            and d.find_elements(By.XPATH, password_input_xpath)
        )
        time.sleep(0.2)

        def fill_input(xpath: str, value: str):
            el = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
            js_click(driver, el)
            time.sleep(0.1)
            try:
                el.clear()
            except Exception:
                # Vuetify inputs sometimes don't clear properly; fall back to select-all delete
                el.send_keys("\ue009" + "a")  # CTRL + A
                el.send_keys("\ue003")  # BACKSPACE
            el.send_keys(value)

        # ---- Email: must be <input type="text"> ----
        fill_input(email_input_xpath, EMAIL)

        # ---- Password: must be <input type="password"> ----
        fill_input(password_input_xpath, PASSWORD)

        # ---- Login button (wait until actually enabled + clickable) ----
        login_btn_xpath = (
            "//button[@type='submit' and contains(@class,'v-btn') "
            "and contains(@class,'v-btn--block') "
            "and contains(@class,'bg-main-01') "
            "and .//span[contains(@class,'v-btn__content') and normalize-space()='ログイン']]"
        )

        wait.until(lambda d: d.find_element(By.XPATH, login_btn_xpath))
        wait.until(
            lambda d: (
                (btn := d.find_element(By.XPATH, login_btn_xpath)).is_enabled()
                and btn.get_attribute("disabled") is None
                and "v-btn--disabled" not in (btn.get_attribute("class") or "")
            )
        )
        login_btn = wait.until(EC.element_to_be_clickable((By.XPATH, login_btn_xpath)))
        js_click(driver, login_btn)
        time.sleep(3)

        # =========================================================
        # 5) AFTER LOGIN: click "この講座のトークページへ" if present
        # =========================================================
        talk_btn_xpath = (
            "//button[contains(@class,'v-btn') and contains(@class,'v-btn--block') "
            "and .//span[contains(@class,'v-btn__content') "
            "and contains(normalize-space(.),'この講座のトークページへ')]]"
        )

        try:
            # Wait a bit for the post-login page to render
            talk_wait = WebDriverWait(driver, 20)
            talk_btn = talk_wait.until(EC.presence_of_element_located((By.XPATH, talk_btn_xpath)))
            talk_btn = talk_wait.until(EC.element_to_be_clickable((By.XPATH, talk_btn_xpath)))
            before_url = driver.current_url
            for _ in range(3):
                js_click(driver, talk_btn)
                try:
                    WebDriverWait(driver, 15).until(
                        lambda d: ("/plan/" in (d.current_url or "")) and ("tab=talk" in (d.current_url or ""))
                    )
                    break
                except Exception:
                    # Re-locate and retry (SPA sometimes drops the click)
                    try:
                        talk_btn = driver.find_element(By.XPATH, talk_btn_xpath)
                    except Exception:
                        pass
                    time.sleep(0.6)

            time.sleep(1.0)
            print("Clicked: この講座のトークページへ")
            print("URL after click:", driver.current_url)

            # If we still didn't reach the talk tab, try clicking a "トーク" tab/button if present
            if "tab=talk" not in (driver.current_url or ""):
                try:
                    talk_tab = driver.find_element(
                        By.XPATH,
                        "//*[self::a or self::button or @role='tab' or @role='button']"
                        "[contains(normalize-space(.),'トーク')]",
                    )
                    js_click(driver, talk_tab)
                    WebDriverWait(driver, 15).until(lambda d: "tab=talk" in (d.current_url or ""))
                    print("Switched to トーク tab via tab/button.")
                except Exception:
                    pass
        except Exception:
            # Not all accounts/pages show this CTA; keep script usable.
            pass

        # =========================================================
        # 6) OPEN TALK THREAD: エントリー&決済タイミング
        # =========================================================
        talk_title = "エントリー&決済タイミング"
        thread_wait = WebDriverWait(driver, 40)
        
        # Wait for talk list to load (look for any clickable items)
        print("[DEBUG] Waiting for talk list to load...")
        try:
            thread_wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a, button, [role='button']"))
            )
            time.sleep(2)  # Extra time for virtualized list to render
            print(f"[DEBUG] Current URL: {driver.current_url}")
            print(f"[DEBUG] Page title: {driver.title}")
        except Exception as e:
            print(f"[DEBUG] Warning: Could not detect talk list elements: {e}")
        
        try:
            open_talk_thread_by_title(driver, thread_wait, talk_title)
        except InvalidSessionIdException:
            raise
        except Exception as e:
            # If the page is already the desired thread, don't fail hard.
            try:
                keywords = [k.strip() for k in re.split(r"[&＆]", talk_title) if k.strip()]
                kw_pred = " and ".join([f"contains(normalize-space(.),'{k}')" for k in keywords])
                already = driver.find_elements(
                    By.XPATH,
                    f"//header//*[{kw_pred}] | "
                    f"//*[contains(@class,'header') or contains(@class,'title') or contains(@class,'talk')][{kw_pred}]",
                )
                if any(visible(x) for x in already):
                    print(f"Already on talk thread (detected by keywords): {talk_title}")
                else:
                    raise RuntimeError(
                        f"Failed to open talk thread '{talk_title}'. "
                        f"Current URL={driver.current_url!r} Title={driver.title!r}. "
                        f"Original error: {e!r}"
                    )
            except Exception:
                raise RuntimeError(
                    f"Failed to open talk thread '{talk_title}'. "
                    f"Current URL={driver.current_url!r} Title={driver.title!r}. "
                    f"Original error: {e!r}"
                )
        print(f"Opened talk thread: {talk_title}")

        # =========================================================
        # 7) TALK THREAD: collect all div.content.isText
        # =========================================================
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.content.isText")))
            time.sleep(0.5)

            texts = collect_all_istext_contents(driver, max_scrolls=2000, pause_s=0.35)
            raw_text_for_db = "\n\n---\n\n".join(texts)

            # =========================================================
            # SAVE TO SQLITE DATABASE
            # =========================================================
            # Split -> hash dedupe -> store -> classify immediately (trading vs non-trading)
            db_result = save_snapshot_and_segments(raw_text_for_db, channel=talk_title)
            print("\n" + "=" * 80)
            print("DATABASE SAVE RESULTS")
            print("=" * 80)
            print(f"Total segments: {db_result['segments_total']}")
            print(f"New segments inserted: {db_result['inserted']}")
            print(f"New trading signals: {db_result['inserted_trading']}")
            print("=" * 80 + "\n")

            # =========================================================
            # OPTIONAL: BACKUP TO TEXT FILE (set to False to disable)
            # =========================================================
            SAVE_TEXT_BACKUP = os.getenv("SAVE_TEXT_BACKUP", "false").lower() == "true"
            
            if SAVE_TEXT_BACKUP:
                out_path = os.path.join(
                    os.getcwd(),
                    f"talk_contents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                )
                with open(out_path, "w", encoding="utf-8") as f:
                    for t in texts:
                        f.write(t.replace("\r\n", "\n").replace("\r", "\n"))
                        f.write("\n\n---\n\n")
                print(f"[INFO] Text backup saved to: {out_path}\n")

            # =========================================================
            # DISPLAY SUMMARY
            # =========================================================
            print(f"[INFO] Collected {len(texts)} messages from talk thread")
            print(f"[INFO] All data saved to SQLite database: data/fincs.db")
            print(f"[INFO] Use 'python src/query_db.py stats' to view database statistics")
            print(f"[INFO] Use 'python src/query_db.py events' to view recent trading events")
            
            # Show first 5 messages as preview
            print("\n" + "=" * 80)
            print("PREVIEW: First 5 messages")
            print("=" * 80)
            for i, t in enumerate(texts[:5], start=1):
                print(f"\n[{i}/{len(texts)}]")
                print(t[:200] + ("..." if len(t) > 200 else ""))
                print("-" * 80)
            if len(texts) > 5:
                print(f"\n... and {len(texts) - 5} more messages (stored in database)")
            print("=" * 80 + "\n")
        except Exception as e:
            print("WARNING: Could not collect talk contents:", repr(e))

        # =========================================================

        print("Login submitted successfully.")
        print("URL:", driver.current_url)
        print("Title:", driver.title)

        if not auto_exit:
            input("If login is successful, press ENTER to close...")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()


# Allow non-interactive scrape
def scrape_once():
    return main(auto_exit=True)
