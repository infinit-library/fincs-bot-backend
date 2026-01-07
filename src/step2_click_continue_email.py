from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

def js_click(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)

def is_visible(el):
    try:
        return el.is_displayed()
    except Exception:
        return False

def get_inner_text(driver, el):
    try:
        txt = driver.execute_script("return arguments[0].innerText || '';", el)
        return (txt or "").strip()
    except Exception:
        return ""

options = webdriver.ChromeOptions()
options.add_argument("--window-size=1400,900")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)
wait = WebDriverWait(driver, 60)

try:
    driver.get("https://fincs.jp/")
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    time.sleep(1)

    # Optional: click header 登録/ログイン if it exists (do not fail if not)
    try:
        labels = driver.find_elements(By.CSS_SELECTOR, ".title-text.text-truncate")
        for el in labels:
            if is_visible(el) and ("ログイン" in (el.text or "") or "登録" in (el.text or "")):
                js_click(driver, el)
                time.sleep(1.5)
                break
    except Exception:
        pass

    # ---- Critical Fix: Find the "Continue with email" button by CLASS SUBSET ----
    # Your provided class list contains these stable pieces:
    #   v-btn, v-btn--block, bg-white
    # We DO NOT match the entire class string.
    candidates = driver.find_elements(By.CSS_SELECTOR, "button.v-btn.v-btn--block.bg-white, a.v-btn.v-btn--block.bg-white")

    # If the element is not a <button>/<a>, fallback to any element with those classes
    if not candidates:
        candidates = driver.find_elements(By.CSS_SELECTOR, ".v-btn.v-btn--block.bg-white")

    if not candidates:
        raise RuntimeError("Could not find any candidate elements matching .v-btn.v-btn--block.bg-white")

    # Choose the best visible candidate:
    # Prefer the one whose innerText includes 'メール' or 'email' (but not required).
    chosen = None
    visible = [el for el in candidates if is_visible(el)]
    if not visible:
        # Vuetify sometimes marks things visible but overlay blocks; still try the first candidate via JS
        visible = candidates

    for el in visible:
        txt = get_inner_text(driver, el)
        if ("メール" in txt) or ("email" in txt.lower()):
            chosen = el
            break

    if not chosen:
        chosen = visible[0]

    # Click it (JS click is most reliable on Vuetify)
    js_click(driver, chosen)
    time.sleep(2)

    # ---- Wait for login form container (Vuetify dialog/overlay/form) ----
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".v-overlay, .v-dialog, form, .v-card")))
    time.sleep(0.5)

    # ---- Find inputs (email might be type=text) ----
    # First: collect visible inputs
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    visible_inputs = [i for i in inputs if is_visible(i)]

    if not visible_inputs:
        # try again after a short delay
        time.sleep(2)
        inputs = driver.find_elements(By.CSS_SELECTOR, "input")
        visible_inputs = [i for i in inputs if is_visible(i)]

    # Identify password input
    password_input = None
    for i in visible_inputs:
        if (i.get_attribute("type") or "").lower() == "password":
            password_input = i
            break

    # Identify email/username input: prefer email/autocomplete/email-like attributes; else first visible text input
    email_input = None
    for i in visible_inputs:
        t = (i.get_attribute("type") or "").lower()
        name = (i.get_attribute("name") or "").lower()
        iid = (i.get_attribute("id") or "").lower()
        ac = (i.get_attribute("autocomplete") or "").lower()
        if t == "email" or ac == "email" or "email" in name or "email" in iid:
            email_input = i
            break

    if not email_input:
        for i in visible_inputs:
            t = (i.get_attribute("type") or "").lower()
            if t in ["text", "email"]:
                email_input = i
                break

    if not email_input or not password_input:
        # Print debug to help pinpoint
        print("DEBUG: visible input count:", len(visible_inputs))
        for idx, i in enumerate(visible_inputs[:10]):
            print(idx,
                  "type=", i.get_attribute("type"),
                  "name=", i.get_attribute("name"),
                  "id=", i.get_attribute("id"),
                  "autocomplete=", i.get_attribute("autocomplete"))
        raise RuntimeError("Login inputs not found after clicking the white v-btn button.")

    print("SUCCESS: Email input found.")
    print("SUCCESS: Password input found.")
    input("Login form is detected. Press ENTER to close...")

finally:
    driver.quit()
