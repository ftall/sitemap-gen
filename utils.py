import re
import string
import random

def is_url_valid(url):
    url_validator = re.compile(
        r'^(?:http)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    return url_validator.match(url) != None

def filename_generator(size=12, chars=string.ascii_uppercase +
                       string.ascii_lowercase + string.digits):
   return ''.join(random.choice(chars) for _ in range(size))
