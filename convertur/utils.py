import re
import requests


def get_code(url):
    # for github url get raw instead of url:
    match = re.match(r'https://github\.com/(?P<namespace>.+?)/(?P<repo>.+?)/blob/(?P<path>.+)', url)
    if match:
        url = 'https://raw.githubusercontent.com/{namespace}/{repo}/{path}'.format(**match.groupdict())
    return requests.get(url).text