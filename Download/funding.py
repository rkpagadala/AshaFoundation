import os
import requests
import re 
from bs4 import BeautifulSoup


def download_data(pid):
    print(f"Downloading data for project {pid}")
    url = f"https://ashanet.org/project/?pid={pid}"
    response = requests.get(url)
    return response.text

# download and save data for 1 to last project(i.e 1353)
def download_all_data():
    i=1
    while True:
        data = download_data(i)
        if not isvalid(i,data):
            break        
        # convert data to html and save it
        if not os.path.exists("Download/HTML_DATA"):
            os.makedirs("Download/HTML_DATA")
        with open(f"Download/HTML_DATA/ashasup_{i}.html", "w") as f:
            f.write(data)
        i += 1
            

def isvalid(dn,data):
    if dn <= 1350:
        return True
    
    stewarding_chapter_pattern = r'<strong>Stewarding Chapter:</strong>\s*<a[^>]*>([^<]*)</a>'
    match = re.search(stewarding_chapter_pattern, data)
    
    if match:
        # Get the content between <a> tags
        chapter_content = match.group(1).strip()
        # Return False if the content is empty
        if chapter_content == "":
            return False
        return True
    return False
