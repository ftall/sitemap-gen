import os
import re
import itertools
import asyncio
import aiohttp
import zipfile

from datetime import datetime
from time import strftime

from urllib.parse import urlparse
from bs4 import BeautifulSoup
from xml.etree import ElementTree as et

from utils import is_url_valid
from conf import DEPTH_LEVEL, REQUEST_TO, REQUEST_RETRY, SELECTOR_LIMIT, SITEMAP_SIZE, XML_PATH

class SitemapObjError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class SitemapObj(object):
    """Simple object to handle entry of Sitemap file.

    SitemapObj Handle the specified URL: make HEAD request to validate the
    response is HTML page, make GET request to obtain the page content,
    collect the links to next level and validate it (valid URL, same domain,
    etc.)

    HTTP Requests done asynchronous, thus correspond methods are coroutines.
    """
    request_retry = REQUEST_RETRY
    request_to = REQUEST_TO
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36",
        "pragma": "no-cache",
        "cache-control": "no-cache",
        "expires": "-1"
    }

    def __init__(self, url):
        self.__url = url
        self.__last_mod = None

    def __eq__(self, other):
        """As SitemapObj could be an element of set object, compare operator
        done work based on the `url` property. Type checks added to support
        URL string and SitemapObj in the same set.
        """
        if isinstance(other, str):
            return self.url == other
        elif isinstance(other, str):
            return self.url == other.url

        raise SitemapObjError("Uncomparable types")

    def __hash__(self):
        return hash(self.url)

    def __str__(self):
        return "%s : %s" % (self.__url, self.__last_mod)

    @property
    def url(self):
        return self.__url

    @asyncio.coroutine
    def __do_request(self, method):
        """Do HTTP request, provide the retry ability if request fails for
        some reason.

        :param method: HTTP method string: "HEAD", "GET", ...
        """
        r = None
        tries = 0
        while tries < self.request_retry:
            try:
                r = yield from asyncio.wait_for(aiohttp.request(method, self.url,
                                                                allow_redirects=True,
                                                                headers = self.headers),
                                                self.request_to)
                break
            except:
                tries += 1

        return r

    @staticmethod
    def validate_update_url(p_url, href, domain, netloc, exclude_root=True):
        """Validate gathered from HTML page links and modify it if required.

        Checked is URL in the same domain, is URL valid itself.
        From URL will be cutted the anchor part (i.e. all after `#` character)

        :param p_url: urlparse object of validated URL
        :param href: just string of link obtained from `href` attribute
        :param domain: domain that validated URL should belong
        :param netloc netloc: netloc that validated URL should belong
        :param exclude_root: True if validated URL should be filtered if it is
                             the same as site root
        """
        exclude_list = [ domain, ]
        exclude_list.append(domain[:-1] if domain[-1] == '/' else domain + '/')

        url = None
        if p_url.netloc == '':
            url = domain + href
        elif p_url.netloc == netloc:
            url = href

        if not url:
            return None
        if not is_url_valid(url):
            return None
        if exclude_root and url in exclude_list:
            return None

        url = url.split("#")[0]

        return url

    @asyncio.coroutine
    def prepare(self):
        """Actual object initialization.

        Could not be done in constructor, because should be a coroutine.

        Do the HTTP HEAD request to check availability of URL. Accepted URL
        is only HTML pages (checked the content-type) and status code is 200.
        HTTP request follow automatically to the HTTP redirect (i.e. 30x status
        code)
        """
        r = yield from self.__do_request("head")
        if not r:
            raise SitemapObjError("SKIP %s: Request exception: timeout, etc." % self.url)

        #Change object URL for the redirect case
        self.__url = r.url

        if r.status != 200:
            raise SitemapObjError("SKIP %s: status code: %d. Unsupported" % (self.url, r.status))

        content_type = r.headers.get('content-type', None)
        if not content_type:
            raise SitemapObjError("SKIP %s: there is no CONTENT-TYPE" % self.url)

        ctype_validator = re.compile(r'text/html')
        if not ctype_validator.match(content_type):
            raise SitemapObjError("SKIP %s: CONTENT-TYPE unsupported" % self.url)

        last_mod = r.headers.get('last-modified', None)
        if last_mod:
            self.__last_mod = datetime.strptime(last_mod, '%a, %d %b %Y %H:%M:%S %Z')

    @asyncio.coroutine
    def process(self, domain, netloc, nextlvl):
        """Analyze content of HTML page.

        Coroutine due to asynchronous HTTP request inside the function.
        Do the GET request and analyze content of page to external hyperlinks.
        Obtained links validated and added to the list that used on the next
        depth level.

        :param domain: domain that validated URL should belong
        :param netloc netloc: netloc that validated URL should belong
        :param nextlvl: list that contain SitemapObj of next search level
        """
        r = yield from self.__do_request("get")
        if not r:
            return

        content = yield from r.text()
        soup = BeautifulSoup(content)

        for a in soup.find_all('a'):
            href = a.get('href')
            up = urlparse(href)
            url = self.validate_update_url(up, href, domain, netloc)

            if url:
                nextlvl.add(url)

    def xml_node(self):
        """Create the XML node object of Sitemap file
        """
        url = et.Element('url')

        loc = et.Element('loc')
        loc.text = self.__url
        url.append(loc)

        if self.__last_mod:
            lastmod = et.Element('lastmod')
            lastmod.text = self.__last_mod.strftime("%Y-%m-%d")
            url.append(lastmod)

        return url

class SitemapWalker(object):
    """Simple object for walking over the interested site and generate
    the XML file.

    Manipulate the SitemapObj during the walking of specified site.
    The generated XML will be deleted in special worker thread after
    the specified in configuration timeout (30 minutes by default).

    All requests done in asynchronous way, but number of concurrent
    requests are limited by the `SELECTOR_LIMIT` parameter.
    Walking algorithm based on BFS algorithm for specified level:
      1. Create two queues: thislvl and nextlvl.
      2. Add to the thislvl URL of the spefied in input URL domain.
      3. Do asynchronous HEAD requests for all entries in `thislvl`.
      4. Collect unique and valid SitemapObj entities obtained on the
         previous step.
      5. Stop walking if target depth level reached.
      5. Do asynchronous GET requests for the objects obtained on the
         step 4 and fill the `nextlvl` list during the HTML content
         analysis.
      6. Assign `nextlvl` to `thislvl` and increase depth index.
      7. Go to step 3.

    Note: index of domain URL is 0.
    """
    depth = DEPTH_LEVEL

    def __init__(self, domain, netloc):
        self.__domain = domain
        self.__netloc = netloc
        self.__visited = set()

    @asyncio.coroutine
    def __head_request_coro(self, url, objs, lvlidx):
        """HEAD request coroutine that make asynchronous request
        to create SitemapObj entity and add it to __visited list
        for valid object.

        :param url: source URL for HEAD request
        :param objs: list of SitemapObj-s entities that will be used
                     for next step of algorithm (i.e. GET requests)
        :param lvlidx: current depth level index
        """
        if url in self.__visited:
            return

        try:
            obj = SitemapObj(url)
            yield from obj.prepare()
        except SitemapObjError as e:
            return

        #Redirect case
        if obj.url != url:
            #Domain change
            if lvlidx == 0:
                self.__netloc = urlparse(obj.url).netloc
                self.__domain = urlparse(obj.url).scheme + "://" + self.__netloc

            up = urlparse(obj.url)
            u = SitemapObj.validate_update_url(up, obj.url, self.__domain,
                                               self.__netloc, exclude_root=lvlidx != 0)
            if not u:
                return

        #Check is this URL already not parsed during another asynchronous request
        if obj.url in self.__visited:
            return

        objs.append(obj)
        self.__visited.add(obj)

    @asyncio.coroutine
    def __get_request_coro(self, o, nextlvl):
        """GET request coroutine.

        Do asynchronous GET request for the SitemapObj entity.
        """
        yield from o.process(self.__domain, self.__netloc, nextlvl)

    def __group_execute_tasks(self, handler, tasks, *args):
        """This is a helper function that split input tasks list to
        sublists that satisfy the `SELECTOR_LIMIT` criteria and start
        the asyncio.Task-s that created step by step for splitted lists.

        :param handler: coroutine for the asyncio.Task creation
        :param tasks: list of the objects to creation asyncio.Task
        :param *args: will be passed as it is to coroutine.
        """
        aiotasks = []
        subtasks = itertools.zip_longest(*[iter(tasks)]*SELECTOR_LIMIT)
        for s in subtasks:
            loop = asyncio.SelectorEventLoop()
            asyncio.set_event_loop(loop)

            del aiotasks[:]
            for v in s:
                if not v:
                    break
                aiotasks.append(asyncio.async(handler(v, *args)))

            loop.run_until_complete(asyncio.wait(aiotasks))
            loop.close()

    def traverse_links(self):
        """This is a function that implements walking algorithm described
        in the class header.
        """
        lvlidx = 0
        thislvl = set([ self.__domain, ])

        while thislvl:
            objs = []
            nextlvl = set()
            self.__group_execute_tasks(self.__head_request_coro, thislvl, objs, lvlidx)

            if lvlidx >= self.depth:
                break

            self.__group_execute_tasks(self.__get_request_coro, objs, nextlvl)

            thislvl = nextlvl
            lvlidx += 1

    @classmethod
    def __indent(cls, elem, level=0):
        """
        copy and paste from http://effbot.org/zone/element-lib.htm#pretty#print
        it basically walks your tree and adds spaces and newlines so the tree is
        printed in a nice way

        Not needed actuall, just to pretiffy XML output
        """
        i = "\n" + level*"  "
        if len(elem):
          if not elem.text or not elem.text.strip():
            elem.text = i + "  "
          if not elem.tail or not elem.tail.strip():
            elem.tail = i
          for elem in elem:
            cls.__indent(elem, level+1)
          if not elem.tail or not elem.tail.strip():
            elem.tail = i
        else:
          if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

    def __generate_sitemap(self, filename, objlist):
        """Helper to generate the Sitemap XML file

        :param filename: name of the file (without ".xml" extension)
        :param objlist: list of SitemapObj entities to create "url" tag
        """
        root = et.Element('urlset')
        root.attrib["xmlns"] = "http://www.sitemaps.org/schemas/sitemap/0.9"

        for l in objlist:
            if not l:
                break
            root.append(l.xml_node())

        self.__indent(root)
        tree = et.ElementTree(root)

        filename += ".xml"
        with open(filename, 'w', encoding='utf-8') as file:
            tree.write(file, encoding='unicode', xml_declaration=True)

        return filename

    def generate_sitemap(self, filename):
        """Generate the Sitemap file for specified filename.

        If the output XML file > 50000 entries it will be splitted
        to different ones and created SitemapIndex file. All files
        will be packed to the ZIP archive.

        :param filename: name of the file (without ".xml" extension)
        """
        if len(self.__visited) <= SITEMAP_SIZE:
            return self.__generate_sitemap(filename, self.__visited)

        root = et.Element('sitemapindex')
        root.attrib["xmlns"] = "http://www.sitemaps.org/schemas/sitemap/0.9"

        zipname = filename + ".zip"
        with zipfile.ZipFile(zipname, 'w') as zipf:
            sublists = itertools.zip_longest(*[iter(self.__visited)]*SITEMAP_SIZE)
            for idx, s in enumerate(sublists):
                sitemap = et.Element('sitemap')
                xml = self.__generate_sitemap("%ssitemap%d" % (XML_PATH, idx), s)

                loc = et.Element('loc')
                loc.text = self.__domain + '/' + xml.split('/')[-1]
                sitemap.append(loc)

                zipf.write(xml)
                os.remove(xml)

                root.append(sitemap)

            self.__indent(root)
            tree = et.ElementTree(root)

            fileidx = filename + ".xml"
            with open(fileidx, 'w', encoding='utf-8') as file:
                tree.write(file, encoding='unicode', xml_declaration=True)

            zipf.write(fileidx)
            os.remove(fileidx)

        return zipname
