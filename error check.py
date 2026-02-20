from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

CREATOR_URL = "https://rapidapi.com/user/hoangdaicntt"
API_URL = "https://rapidapi.com/hoangdaicntt/api/tiktok-shop-analysis"

#This function should go to the API creators URL and check when the API was last updated
def last_updated():
    try:
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(30)
        driver.get(CREATOR_URL)
        updated = driver.find_element(By.XPATH, "//span[contains(text(),'Updated')]")

        updated_time = updated.text.strip()
        updated_time = updated_time[8:len(updated_time)-5]

        if "year" in updated_time:
            approx_days = 365 * int(updated_time[0:updated_time.find(" ")])
        elif "month" in updated_time:
            approx_days = 30 * int(updated_time[0:updated_time.find(" ")])
        elif "week" in updated_time:
            approx_days = 7 * int(updated_time[0:updated_time.find(" ")])
        elif "day" in updated_time:
            approx_days = int(updated_time[0:updated_time.find(" ")])
        else:
            approx_days = 0
            
        if approx_days < 100:
            return f"Recently updated, and {check_methods()}"
        elif approx_days > 180:
            return "API has not been updated in over 6 months; it may have been abandoned"
        else:
            return "API has been updated in the past 6 months; it may need to be updated. Try program again later"

    except Exception as e:
        return f"{CREATOR_URL} could not be opened. Please check that you are connected to the internet, and if this continues to fail, check that the user still exists on RapidAPI."


#This function should go to the API and confirm that the List Top Products method is still present
def check_methods():
    try:
        driver = webdriver.Chrome()
        driver.implicitly_wait(30)
        driver.get(API_URL)
        updated = " "
        updated = driver.find_element(By.XPATH, "//div[normalize-space()='List Top Products']")

        if updated.text == "List Top Products":
            return f"{updated.text} is still listed as a method of the API"
        return "the List Top Products method is not found"

    except Exception as e:
        return f"{API_URL} could not be opened. Please check that you are connected to the internet, and if this continues to fail, check that the user still exists on RapidAPI."