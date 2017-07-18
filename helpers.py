import dateutil.parser
import json
import re
import subprocess
import time

import requests

from hashlib import sha1

from requests.exceptions import ConnectionError

from platform import system as system_name  # Returns the system/OS name
from os import system as system_call        # Execute a shell command


# ################# GLOBAL VARIABLES ###########################

# Set the headers
_HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'User-Agent': (
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ' (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
    )
}

# Set the maximum number of retries per request before giving up on the request
_MAX_RETRIES = 4

# Set the sleep time between retries
_RETRY_SLEEP = 1

# Set the maximum number of requests before waiting for a period of time
_MAX_REQUESTS = 2000

# Set the wait time in seconds
_MAX_WAIT_TIME = 900
_AVG_WAIT_TIME = 5
_MIN_WAIT_TIME = 2

# Month mapping from romanian to english
_MONTHS_RO_EN = {
    'ianuarie': 'January',
    'februarie': 'February',
    'martie': 'March',
    'aprilie': 'April',
    'mai': 'May',
    'iunie': 'June',
    'iulie': 'July',
    'august': 'August',
    'septembrie': 'September',
    'octombrie': 'October',
    'noiembrie': 'November',
    'decembrie': 'December',
}


# Set the proxy variables
_PROXIES = {
    'https': None,
}

# Set the number of remaining requests before the program should wait
_REMAINING_REQUESTS = _MAX_REQUESTS

# Set the request success code variable
_REQUEST_SUCCESS = 200

# Set the request timeout
_REQUEST_TIMEOUT = 15.0

# Set the session variable to none initially
_SESSION = None

# ################### HELPER FUNCTIONS #########################


def ping_host(host):

    """
    Returns True if host (str) responds to a ping request.
    Remember that some hosts may not respond to a ping request even if the host name is valid.
    """

    # Ping parameters as function of OS
    if system_name().lower() == "windows":
        parameters = "-n 1"
        command = 'ping {0} {1}'.format(parameters, host)
    else:
        command = ['ping', '-c', '1', host]

    # Build the command string
    # Pinging
    return subprocess.call(command, stdout=subprocess.PIPE) == 0


def check_proxies():
    """
    This function checks the proxies and updates their availability it should be run at the very
    beginning of every scraper.
    """

    proxy_list = eval(open('proxy_list.txt').read())
    for index, proxy in enumerate(proxy_list):
        if not proxy['string']:
            continue

        proxy_list[index]['active'] = 0
        if ping_host(proxy['ip']):
            proxy_list[index]['active'] = 1

    with open('proxy_list.txt', 'w') as proxy_write:
        proxy_write.write(json.dumps(proxy_list))


def get_proxy():
    proxy_list = eval(open('proxy_list.txt').read())
    proxy_index = eval(open('proxy_index.txt').read())
    while proxy_index < len(proxy_list):
        if proxy_list[proxy_index]['active']:
            break
        if proxy_index + 1 < len(proxy_list):
            proxy_index += 1
        else:
            proxy_index = 0

    with open('proxy_index.txt', 'wt') as proxy_index_file:
        if proxy_index + 1 < len(proxy_list):
            proxy_index_file.write(str(proxy_index + 1))
        else:
            proxy_index_file.write('0')

    proxy = proxy_list[proxy_index]
    if not proxy['string']:
        return None

    return proxy['string']


def session_request(url, entity=None, get_response=True, rtype=None, params=None):
    global _REMAINING_REQUESTS, _SESSION

    if not _SESSION:
        _SESSION = requests.session()

    def request(url, get_response=True, rtype=None, params=None):
        global _REMAINING_REQUESTS

        response = None
        retry = 0
        while retry < _MAX_RETRIES:
            _REMAINING_REQUESTS -= 1
            try:
                if rtype.lower() == 'post' and params:
                    response = _SESSION.post(url, data=params, timeout=_REQUEST_TIMEOUT, proxies=_PROXIES)

                elif not params and rtype.lower() == 'post':
                    raise ValueError('The params argument must be provided when performing a post request.')

                elif not rtype or rtype.lower() == 'get':
                    response = _SESSION.get(url, timeout=_REQUEST_TIMEOUT, proxies=_PROXIES)

                break
            except ConnectionError:
                retry += 1
                time.sleep(_MIN_WAIT_TIME)

        if get_response:
            return response

    if _REMAINING_REQUESTS:

        return request(url, get_response=get_response, rtype=rtype, params=params)
    else:
        time.sleep(_AVG_WAIT_TIME)
        _REMAINING_REQUESTS = _MAX_REQUESTS


def request(url, get_response=True, rtype=None, params=None, proxies=True):
    """
    Function for performing requests. It handles retries and waiting
    if the maximum number of requests has been reached.

    :param url: string containing the url where the request is targeted
    :param get_response: A boolean indicating if the request is just for show or
    if the response is usable
    :param rtype: string describing the type of request to be performed
    :param params: a dictionary potentially containing the parameters required to get the result
    :return: String containing the source of a web page
    """

    global _REMAINING_REQUESTS, _PROXIES

    if not _PROXIES['https'] and proxies:
        _PROXIES['https'] = get_proxy()

    # Local request function handles retries
    def _request(url, get_response=True, rtype=None, params=None):
        global _REMAINING_REQUESTS
        retry_no = 0
        while retry_no < _MAX_RETRIES:
            try:
                if rtype == 'post' and params:
                    response = requests.post(url, data=params, proxies=_PROXIES)
                    if response.status_code == _REQUEST_SUCCESS:
                        _REMAINING_REQUESTS -= 1
                        return response.text
                    else:
                        retry_no += 1
                        _REMAINING_REQUESTS -= 1
                        time.sleep(_RETRY_SLEEP)

                elif not params and rtype == 'post':
                    raise ValueError('The params argument must be provided when performing a post request.')

                elif not rtype or rtype == 'get':
                    response = requests.get(url, proxies=_PROXIES)
                    if response.status_code == _REQUEST_SUCCESS:
                        _REMAINING_REQUESTS -= 1
                        return response.text
                    else:
                        retry_no += 1
                        _REMAINING_REQUESTS -= 1
                        time.sleep(_RETRY_SLEEP)
            except:
                _REMAINING_REQUESTS -=1
        return 0

    if _REMAINING_REQUESTS:
        return _request(url, get_response=get_response, rtype=rtype, params=params)
    else:
        _REMAINING_REQUESTS = _MAX_REQUESTS
        if proxies:
            _PROXIES['https'] = get_proxy()
        time.sleep(_MAX_WAIT_TIME)
        return _request(url)


def get_date_text(text):
    """Retrieve date text or None from random text"""

    date_text = text

    if re.match('[0-9]+\s*(.*)\s*[0-9]+', date_text):
        month = re.match('[0-9]+\s*(.*)\s*[0-9]+',date_text).groups()[0].strip()
        date_text = re.sub(month, _MONTHS_RO_EN[month], date_text).strip()

    try:
        parsed_time = dateutil.parser.parse(date_text)
        parsed_time = parsed_time.replace(tzinfo=None)
        return parsed_time.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def emit_id_params(county: str, city: str, id_no:str='', hstring: str='') -> tuple:
    if not id_no:
        hash = sha1(hstring)
        return (county, city, hash)
    return (county, city, id_no)
