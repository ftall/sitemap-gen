import os
import queue
import threading
import time
import asyncio

from urllib.parse import urlparse
from flask import Flask, request, render_template, jsonify, url_for, send_from_directory

from conf import XML_DELETE_TO, XML_QUEUE_SIZE, XML_URL, XML_PATH
from utils import is_url_valid, filename_generator
from sitemap_obj import SitemapWalker

#PYTHONASYNCIODEBUG = 1

delete_queue = queue.Queue(XML_QUEUE_SIZE)
app = Flask(__name__, static_url_path='')
app.debug = True

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate/', methods=['GET'])
def sitemap_gen():
    """Main handler that do actual work Sitemap creation.

    Try to analyze specified site and generate Sitemap file.
    For analysis takes only domain part of URL.
    URL to generetad XML file added to `delete_queue` that
    handle in separate thread in infinite loop to remove the
    old generated XML files.
    """
    if delete_queue.full():
        return jsonify(status="error",
                       msg="ERROR: XML file Queue is full. Please wait until old file will be deleted")

    url = request.args.get("url", None)
    if not url:
        return jsonify(status="error", msg="ERROR: Please specify URL")

    if not is_url_valid(url):
        return jsonify(status="error", msg="ERROR: Incorrect URL specified \"%s\"" % url)

    netloc = urlparse(url).netloc
    domain = urlparse(url).scheme + "://" + netloc

    walker = SitemapWalker(domain, netloc)
    filepath = XML_PATH + filename_generator()

    walker.traverse_links()
    outfile = walker.generate_sitemap(filepath)

    delete_queue.put({ "ts": time.time(), "filename": outfile })

    return jsonify(status="ok", url=url_for('static', filename=outfile),
                   to=XML_DELETE_TO)

@app.route(XML_URL+'<path:path>')
def send_xml(path):
    return send_from_directory('xml', path)

@asyncio.coroutine
def delete_coro(queue):
    """Delete thread coroutine.

    Most of the time in sleep state: wait for the arriving
    new element in Queue or wait timeout to delete the file
    (30 minutes by default)
    """
    while True:
        xml_file = queue.get(block=True)
        sleep_to = XML_DELETE_TO - (time.time() - xml_file["ts"])
        if sleep_to > 0:
            yield from asyncio.sleep(sleep_to)

        try:
            os.remove(xml_file["filename"])
        except OSError:
            pass

def delete_worker(queue):
    """Handler of special thread that used for removing old links
    of generated XML Sitemap files.
    """
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)

    asyncio.Task(delete_coro(queue))
    try:
        loop.run_forever()
    except:
        loop.close()

if __name__ == '__main__':
    #Clean up XML directory on start
    filelist = [ XML_PATH + f for f in os.listdir(XML_PATH) if f.endswith((".xml", ".zip")) ]
    for f in filelist:
        os.remove(f)

    t = threading.Thread(target=delete_worker, args=(delete_queue,))
    t.daemon = True

    t.start()
    app.run()
