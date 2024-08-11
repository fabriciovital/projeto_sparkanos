from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from services import services
import pytest

@pytest.fixture(scope="module")
def driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Executar em modo headless
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    yield driver
    driver.quit()

@pytest.mark.parametrize("service_name, service_info", services.items())
def test_page_load(driver, service_name, service_info):
    url, expected_title = service_info
    try:
        # Open the page
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.title_is(expected_title))
        print(f"Page '{service_name}' loaded successfully.")
    except TimeoutException:
        pytest.fail(f"The page '{service_name}' did not load in time or the expected title was not found.")
    except Exception as e:
        pytest.fail(f"Failed to load the page '{service_name}': {str(e)}")
