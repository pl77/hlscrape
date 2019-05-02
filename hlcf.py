import os
from time import sleep
from bs4 import BeautifulSoup
from urllib.parse import unquote, quote
from requests import Session
import requests
from requests.auth import HTTPProxyAuth
from requests.exceptions import HTTPError, SSLError, ProxyError, ConnectionError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import arrow
from queue import Queue
from threading import Thread, Event
import warnings
import urllib3
import cfscrape
import apsw
from higherintellect.settings import APIKEY, WORKERS, TARGETDOMAIN
import json


warnings.simplefilter('ignore', urllib3.exceptions.SecurityWarning)
databaseurl = 'hlcf.db'
DirectorySet = set()
FileSet = set()
cloudflare = dict(flag=False)
stoppingenc = Event()
stoppingdl = Event()

rootdir = r'D:\hl'


class DownloadWorker(Thread):
    def __init__(self, queue, http, dbqueue):
        Thread.__init__(self)
        self.queue = queue
        self.dbqueue = dbqueue
        self.http = http
        self.response = None

    def run(self):
        cferror = False
        self.response = None
        cafile = 'crawlera-ca.crt'
        while not stoppingdl.is_set() and not cferror:
            # Get the work from the queue and expand the tuple
            url = self.queue.get()
            proxy_host = "proxy.crawlera.com"
            proxy_port = "8010"
            proxies = {"http": "http://{}:@{}:{}/".format(APIKEY, proxy_host, proxy_port),
                       "https": "http://{}:@{}:{}/".format(APIKEY, proxy_host, proxy_port)}
            requrl = quote(url).replace('%3A', ':')
            banned = True

            try:
                while banned:
                    self.response = requests.get(requrl,
                                                 proxies=proxies,
                                                 verify=cafile)
                    if 'x-crawlera-error' not in self.response.headers.keys():
                        banned = False
                    elif self.response.status_code == 404:
                        banned = False
            except ConnectionError:
                self.queue.put(url)
                self.queue.task_done()
                continue
            sc = self.response.status_code
            if url.endswith('../') or url.endswith('#s'):
                self.queue.task_done()
                continue
            if sc == 200:

                self.parse()
                clean_url = unquote(url)
                dirname = clean_url.replace('https://{}/texts/'.format(TARGETDOMAIN), '')
                dirpath = os.path.join(rootdir, dirname)
                try:
                    if not os.path.isdir(dirpath):
                        os.makedirs(dirpath, exist_ok=True)
                    fpathname = os.path.join(rootdir, 'index.html')
                    if os.path.isfile(fpathname):
                        continue
                    with open(fpathname, 'w', encoding='UTF8') as fp:
                        fp.write(self.response.text)
                except OSError:
                    pass
                self.queue.task_done()
                continue
            elif sc == 404:
                print("Status Code", sc, "Download failed:", url, requrl)
                self.queue.task_done()
                continue
            elif sc == 504 or sc == 403 or sc == 502 or sc == 503:
                print("Status Code", sc, "Download failed due to server error:", url, requrl)
                print(self.response.headers, '\n', self.response.request)
                self.queue.put(url)
                self.queue.task_done()
                sleep(5)
                continue
            else:
                print("Status Code", sc, "Download failed:", url)
                self.queue.put(url)
                self.queue.task_done()
                continue

    def parse(self):
        soup = BeautifulSoup(self.response.text, 'lxml')
        files = soup.find_all("tr", class_="file")
        directories = soup.find_all("tr", class_="dir")
        for file in files:
            resp_url = unquote(self.response.url)
            name_element = file.td
            name = name_element.text
            path = "{rt}{nm}".format(rt=resp_url, nm=name)
            if path in FileSet:
                continue
            type_element = name_element.find_next_sibling()
            size_element = type_element.find_next_sibling()
            size = int(size_element.attrs['sorttable_customkey'])
            date_element = size_element.find_next_sibling()
            date = arrow.get(date_element.attrs['sorttable_customkey'], 'YYYYMMDDHHmmss')
            filedict = dict(name=name, size=size, date=date.datetime, path=path, type='file')
            self.dbqueue.put(filedict)
            FileSet.add(filedict['path'])
        for directory in directories:
            resp_url = unquote(self.response.url)
            name_element = directory.td
            name = name_element.text
            path = "{rt}{nm}".format(rt=resp_url, nm=name)
            if name == '../' or name == '#s':  # path in DirectorySet
                continue
            type_element = name_element.find_next_sibling()
            size_element = type_element.find_next_sibling()
            # date_element = size_element.find_next_sibling()
            date = None # arrow.get(date_element.attrs['sorttable_customkey'], 'YYYYMMDDHHmmss')
            dirdict = dict(name=name, date=date, path=path, type='dir')
            self.dbqueue.put(dirdict)
            self.queue.put(dirdict['path'])
            DirectorySet.add(dirdict['path'])


class DBWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.name = 'hlcf.db'

    def run(self):

        while True:
            # Get the work from the queue and expand the tuple
            response = self.queue.get()
            if response is not None:
                if response['type'] == 'dir':
                    apsw_directory_query(response)

                elif response['type'] == 'file':
                    apsw_file_query(response)

            self.queue.task_done()


class AlreadyDownloadedException(Exception):
    pass


def _pragma_on_connect(dbapi_con):
    # dbapi_con.execute('pragma foreign_keys=ON')
    dbapi_con.execute("PRAGMA journal_mode = WAL")
    dbapi_con.execute("PRAGMA synchronous = NORMAL")
    dbapi_con.execute("PRAGMA temp_store = MEMORY")
    # dbapi_con.execute("PRAGMA cache_size = 1000000")


# event.listen(engine, 'connect', _pragma_on_connect)


def stream_url(url, whttp, **kwargs):
    """
    Return a request's Response object for the given URL
    """
    # crawleraurl = 'proxy.crawlera.com: 8010 - U '
    """
    headers = {'User-Agent': 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1;  '
                             '+http://www.google.com/bot.html) Safari/537.36.'
               }
    """
    proxy_host = "proxy.crawlera.com"
    proxy_port = "8010"
    proxy_auth = HTTPProxyAuth(APIKEY, "")
    proxies = {"http": "http://{}:{}/".format(proxy_host, proxy_port),
               "https": "http://{}:{}/".format(proxy_host, proxy_port)}

    return whttp.get(url, stream=True, proxies=proxies, auth=proxy_auth, verify='crawlera-ca.crt', **kwargs)


def apsw_grab_dbsets():
    with apsw.Connection(databaseurl) as db:
        db.setbusytimeout(60000)
        cursor = db.cursor()
        cursor.execute("pragma journal_mode=wal")
        cursor.execute("pragma syncronous=off")
        print("Accessing DB Directories")
        directories = list(cursor.execute("SELECT id, path FROM directories"))
        dbdirdict = dict()
        for dirtuple in tqdm(directories):
            dbdirdict[dirtuple[1]] = dirtuple[0]
        print("Accessing DB Files")

        files = list(cursor.execute("SELECT id, path FROM files"))
        dbfiledict = dict()
        for filetuple in tqdm(files):
            dbfiledict[filetuple[1]] = filetuple[0]

    return dbdirdict, dbfiledict


def apsw_directory_many(dirdictlist):
    insertstmt = "INSERT OR IGNORE INTO directories (path, date, scraped, visited) VALUES (:a, :b, :c, :d)"
    now = arrow.now().timestamp
    insertdictlist = list()
    for dirdict in dirdictlist:
        dirdict['scraped'] = now
        dirdict['visited'] = now
        insertstmtdict = dict(a=dirdict['path'],
                              b=str(dirdict['date']),
                              c=str(dirdict['scraped']),
                              d=str(dirdict['visited']))
        insertdictlist.append(insertstmtdict)
    with apsw.Connection(databaseurl) as db:
        db.setbusytimeout(60000)
        cur = db.cursor()
        cur.execute("pragma journal_mode=wal")
        cur.execute("pragma syncronous=off")
        db.setbusytimeout(60000)
        cur.executemany(insertstmt, insertdictlist)


def apsw_file_many(filedictlist):
    insertstmt = "INSERT OR IGNORE INTO files (path, size, date, scraped, visited) VALUES (:a, :b, :c, :d, :e)"
    now = arrow.now().timestamp
    insertdictlist = list()
    for filedict in filedictlist:
        filedict['scraped'] = now
        filedict['visited'] = now
        insertstmtdict = dict(a=filedict['path'],
                              b=filedict['size'],
                              c=str(filedict['date']),
                              d=str(filedict['scraped']),
                              e=str(filedict['visited'])
                              )
        insertdictlist.append(insertstmtdict)
    with apsw.Connection(databaseurl) as db:
        db.setbusytimeout(60000)
        cur = db.cursor()
        cur.execute("pragma journal_mode=wal")
        cur.execute("pragma syncronous=off")
        db.setbusytimeout(60000)
        cur.executemany(insertstmt, insertdictlist)


def apsw_directory_query(dirdict):
    now = arrow.now().timestamp
    dirdict['scraped'] = now
    dirdict['visited'] = now
    insertstmt = "INSERT OR IGNORE INTO directories (path, date, scraped, visited) VALUES (:a, :b, :c, :d)"
    insertstmtdict = dict(a=dirdict['path'],
                          b=str(dirdict['date']),
                          c=str(dirdict['scraped']),
                          d=str(dirdict['visited']))
    with apsw.Connection(databaseurl) as db:
        db.setbusytimeout(60000)
        cur = db.cursor()
        cur.execute("pragma journal_mode=wal")
        cur.execute("pragma syncronous=off")
        cur.execute(insertstmt, insertstmtdict)


def apsw_file_query(filedict):
    now = arrow.now().timestamp
    filedict['scraped'] = now
    filedict['visited'] = now

    insertstmt = "INSERT OR IGNORE INTO files (path, size, date, scraped, visited) VALUES (:a, :b, :c, :d, :e)"
    insertstmtdict = dict(a=filedict['path'],
                          b=filedict['size'],
                          c=str(filedict['date']),
                          d=str(filedict['scraped']),
                          e=str(filedict['visited'])
                          )
    with apsw.Connection(databaseurl) as db:
        db.setbusytimeout(60000)
        cur = db.cursor()
        cur.execute("pragma journal_mode=wal")
        cur.execute("pragma syncronous=off")
        cur.execute(insertstmt, insertstmtdict)


def attempt_cloudflare(url, cfhttp):
    response = stream_url(url, cfhttp)
    status = response.status_code
    response.close()
    if status == 503:
        cloudflare['flag'] = True
        print("503 Error code received, attempting Cloudflare scrape")
        response = stream_url(url, cfhttp)
        status = response.status_code
        response.close()
    return status


def get_cfcookies(url, cfhttp):
    # http.headers["X-Crawlera-Session"] = headers["X-Crawlera-Session"]
    proxy_host = "proxy.crawlera.com"
    proxy_port = "8010"
    proxy_auth = HTTPProxyAuth(APIKEY, "")
    proxies = {"https": "https://{}:{}/".format(proxy_host, proxy_port),
               "http": "http://{}:{}/".format(proxy_host, proxy_port)}
    # response = http.get(url, stream=True, proxies=proxies, auth=proxy_auth, verify='crawlera-ca.crt')
    try:
        tokens = cfhttp.get_tokens(url, proxies=proxies, auth=proxy_auth, verify='crawlera-ca.crt')
    except (ValueError, SSLError, ProxyError, HTTPError):
        tokens = None
    # print('x-crawlera-session=', http.headers["X-Crawlera-Session"], 'User-Agent', http.headers["User-Agent"])
    return tokens


def load_pagelist():
    pagelist = list()
    with open('higher.txt', 'r', encoding='UTF8') as rfile:
        for line in rfile:
            line = line.rstrip()
            pagelist.append(line)
    return pagelist


def create_dlworker(number, queue, db_queue):
    # print("Starting Worker", number)
    # with cfscrape.create_scraper() as http:
    with Session() as http:
        retry = Retry(connect=5, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        http.mount('http://', adapter)
        http.mount('https://', adapter)
        """
        # print("Created Scraper for worker", number)
        session_header = "X-Crawlera-Session"
        session_id = "create"
        headers = {  # session_header: session_id,
                   #"X-Crawlera-UA": "pass",
                   #"X-Crawlera-Cookies": "disable",
                   # "X-Crawlera-No-Bancheck": "1",
                   # "X-Crawlera-Timeout": "180000",
                   #"Referer": "https://{}/texts/".format(TARGETDOMAIN)
                   }
        # tokens = (dict(), ua)
        """
        proxy_host = "proxy.crawlera.com"
        proxy_port = "8010"
        proxy_auth = HTTPProxyAuth(APIKEY, "")
        proxies = {"https": "https://{}:{}/".format(proxy_host, proxy_port),
                   "http": "http://{}:{}/".format(proxy_host, proxy_port)}
        http.proxies = proxies
        http.auth = proxy_auth
        http.verify = 'crawlera-ca.crt'
        """
        #http.headers.update(headers)
        # print("Connecting to httpbin.org/ip for session id for worker", number)

        try:
            with http.get("http://httpbin.scrapinghub.com/ip") as crawlerarequest:
                crawlerasessid = crawlerarequest.headers.get('x-crawlera-session')
        except KeyError:
            return
        # print('x-crawlera-session=', crawlerasessid, "user-agent", ua)
        # print("Establishing Crawlera Session for worker", number)
        # http.headers.update({session_header: crawlerasessid})
        # cookie_arg, user_agent = cfscrape.get_cookie_string("https://{}/texts/".format(TARGETDOMAIN))
        # http.headers.update({'Cookies': cookie_arg})
        # http.headers.update({'User-Agent': user_agent})
        # print("Starting DownloadWorker Process for worker", number)
        """
        worker = DownloadWorker(queue, http, db_queue)
        worker.daemon = True
        worker.start()


def scrapehigherintellect():
    tmpurl = 'https://{}/texts/{}/'
    pagelist = list()
    rootlist = load_pagelist()
    for rt in rootlist:
        pagelist.append(tmpurl.format(TARGETDOMAIN, rt))
    queue = Queue()
    # cookie_arg, user_agent = cfscrape.get_cookie_string("https://{}/".format(TARGETDOMAIN))
    dbdirdict, dbfiledict = apsw_grab_dbsets()
    db_queue = Queue(maxsize=1000)
    for x in range(1):
        dbworker = DBWorker(db_queue)
        dbworker.daemon = True
        dbworker.start()

    print("CFSCRAPE VERSION:", cfscrape.__version__)
    dbdirpaths = set(dbdirdict.keys())
    for page in pagelist:
        dbdirpaths.add(page)
    for pth in dbdirpaths:
        queue.put(pth)
    for wrkr in range(WORKERS):
        create_dlworker(wrkr, queue, db_queue)
    queue.join()
    db_queue.join()
    stoppingdl.set()


def parse_lsjson():
    fpath = r'D:\lsjsonoutput.txt'
    with open(fpath, 'r', encoding='UTF8') as jd:
        js = json.loads(jd.read())
    dirdictlist = list()
    filedictlist = list()
    for record in tqdm(js):
        if record['IsDir']:
            ddate = arrow.get(record['ModTime'])
            dirdict = dict()
            dirdict['path'] = 'https://{}/{}/'.format(TARGETDOMAIN, record['Path'])
            dirdict['date'] = str(ddate.datetime)
            dirdict['type'] = 'dir'
            dirdictlist.append(dirdict)
            # apsw_directory_query(dirdict)

        else:
            filedict = dict()
            fdate = arrow.get(record['ModTime'])
            filedict['path'] = 'https://{}/{}'.format(TARGETDOMAIN, record['Path'])
            filedict['size'] = record['Size']
            filedict['date'] = str(fdate.datetime)
            filedict['type'] = 'file'
            filedictlist.append(filedict)
            # apsw_file_query(filedict)
    print("Inserting directory records...")
    apsw_directory_many(dirdictlist)
    print("Inserting File records...")
    apsw_file_many(filedictlist)


def main():
    scrapehigherintellect()
    # parse_lsjson()


if __name__ == '__main__':
    main()
