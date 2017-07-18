import re
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup

import helpers


BASE_URL = 'http://www.nspa.nato.int/en/organization/procurement/contract.htm'


def download_pdfs():
    """
    Download the NATO Support and Procurement Agency Contract Awards
    pdf lists.
    """


    response = helpers.request(BASE_URL)
    soup = BeautifulSoup(response, 'lxml')

    pdf_links = soup.select('div.boxContent ul li a')
    for link in pdf_links:
        name = link['href']
        name = 'pdfs/' + re.sub('/PDF/Procurement/', '', name)

        link = urljoin(BASE_URL, link['href'])
        pdf_response = requests.get(link)
        with open(name, 'wb') as pdf:
            for chunk in pdf_response.iter_content():
                if chunk:
                    pdf.write(chunk)


def main():
    download_pdfs()


if __name__ == '__main__':
    main()