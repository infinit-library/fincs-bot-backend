from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

options = webdriver.ChromeOptions()
options.add_argument("--window-size=1400,900")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

wait = WebDriverWait(driver, 30)

driver.get("https://fincs.jp/")
wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

# 1) Find the label node (the text element)
label = wait.until(
    EC.presence_of_element_located((By.CSS_SELECTOR, ".title-text.text-truncate"))
)
driver.execute_script("arguments[0].scrollIntoView({block:'center'});", label)

# 2) Climb to a "clickable" ancestor (broader than a/button)
# Priority:
#   - a/button
#   - role=button
#   - onclick attribute
#   - otherwise nearest div/li that is displayed
xpath_clickable = """
./ancestor-or-self::*[
    self::a or self::button
    or @role='button'
    or @onclick
][1]
"""

click_target = None
candidates = label.find_elements(By.XPATH, xpath_clickable)

if candidates:
    click_target = candidates[0]
else:
    # Fallback: climb to nearest reasonable container (div/li) and click it
    fallback_xpath = "./ancestor-or-self::div[1] | ./ancestor-or-self::li[1]"
    fallback_candidates = label.find_elements(By.XPATH, fallback_xpath)
    if fallback_candidates:
        click_target = fallback_candidates[0]

if not click_target:
    raise RuntimeError("Could not find a clickable parent for the login label.")

driver.execute_script("arguments[0].scrollIntoView({block:'center'});", click_target)

# 3) Try normal click, then JS click if blocked
try:
    wait.until(EC.element_to_be_clickable(click_target))
    click_target.click()
except Exception:
    driver.execute_script("arguments[0].click();", click_target)

input("Clicked login area. Confirm the login form appears. Press ENTER to close...")
driver.quit()
