''' Tools
'''

import re
import requests


def get_code(url):
    ''' Get code via https. For github url get raw instead of url.
    '''
    match = re.match(
        r'https://github\.com/(?P<namespace>.+?)/(?P<repo>.+?)/blob/(?P<path>.+)',
        url
    )
    if match:
        url = 'https://raw.githubusercontent.com/{namespace}/{repo}/{path}'
        url = url.format(**match.groupdict())
    return requests.get(url, timeout=300).text
