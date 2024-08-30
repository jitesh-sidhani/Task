# with headless

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options

# Set up Chrome options for headless mode
options = Options()
options.add_argument("--headless=new")

# Set up Chrome service
service = ChromeService()

def login(driver, username, password):
    login_button = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.XPATH, '//*[contains(concat( " ", @class, " " ), concat( " ", "account", " " ))]'))
    )
    login_button.click()
    email_input = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.XPATH, '//*[(@id = "id_username")]'))
    )
    password_input = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.XPATH, '//*[(@id = "id_password")]'))
    )
    email_input.send_keys(username)
    password_input.send_keys(password)
    second_login_button = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.XPATH, '//*[contains(concat( " ", @class, " " ), concat( " ", "icon-user", " " ))]'))
    )
    second_login_button.click()



driver = webdriver.Chrome(service=service, options=options)
driver.maximize_window()
driver.get("https://www.screener.in/company/RELIANCE/consolidated/")

export = WebDriverWait(driver, 5).until(
    EC.element_to_be_clickable((By.XPATH, '//*[contains(concat( " ", @class, " " ), concat( " ", "icon-download", " " ))]'))
)

login(driver, 'firstscreener123@gmail.com','Asdf!234')
# login(os.environ['VAULT_USERNAME'], os.environ['VAULT_PASSWORD'])

export = WebDriverWait(driver, 5).until(
    EC.element_to_be_clickable((By.XPATH, '//*[contains(concat( " ", @class, " " ), concat( " ", "icon-download", " " ))]'))
)
export.click()