# coding:utf-8

"""
A spider for porn sites: www.avmoo.net, www.avless.net, www.avmemo.net
"""

import re
import random
import socket
import signal
import requests
import MySQLdb
import _mysql_exceptions
from time import time
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count, current_process


def log(msg):
    """
    日志前加入进程名称
    :param msg:
    :return:
    """
    name = current_process().name
    print '[%s] %s' % (name, msg)


class Spider(object):
    headers = {
        'Host': '',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'Accept': 'text / html, application / xhtml + xml, application / xml;'
                  'q = 0.9, image / webp, * / *;q = 0.8',
        'User-Agent': '',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2',
    }

    def __init__(self, domain, proxies=None):
        # 网站信息
        self.proxies = proxies
        self.domain = domain
        self.server = domain + '/cn'
        self.ptn_server = self.server.replace('.', '\.')
        self.headers['Host'] = domain

    def do_get(self, url, headers=None, use_proxy=True):
        """
        获取源码
        :param headers:
        :param url: URL
        :param hds: HTTP 请求头
        :param use_proxy: 第一次请求是否使用代理
        :return: 源码
        """
        if headers is None:
            headers = self.headers

        agent = Agent.get_agent()
        headers = headers.copy()
        headers['User-Agent'] = agent

        retry = 0
        res = ''

        while True:
            try:
                if self.proxies:
                    proxy = random.choice(self.proxies)  # 随机选取代理
                    proxies = {'http': proxy}
                else:
                    use_proxy = False

                if not use_proxy:
                    res = requests.get(url, headers=headers)
                else:
                    res = requests.get(url, headers=headers, proxies=proxies)
            except requests.exceptions.Timeout:
                log('[HTTP: Timeout]')
            except (requests.exceptions.HTTPError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.RequestException,
                    socket.error):
                log('[HTTP: Exception]')
                continue

            # 判断返回码
            code = res.status_code
            if code != 200:
                log('[HTTP: %d] retrying......' % code)
                if not use_proxy:  # 重试时用代理
                    use_proxy = True

                # 重试一定次数则跳出循环，返回
                retry += 1
                if retry > 10:
                    return ''
            else:
                return res.text  # 跳出循环，返回源码

    def get_latest(self, page=1):
        """
        分页获取最新的作品链接
        :param page:
        :return:
        """
        url_latest = 'http://%s/currentPage/{page}' % self.server
        ptn_movie_href = 'http://%s/movie/(.*)' % self.ptn_server

        res = self.do_get(url_latest.format(page=page), headers=self.headers)
        soup = BeautifulSoup(res)
        items = soup.find_all('div', class_='item')

        result = []
        for item in items:
            mid = re.search(ptn_movie_href, item.a['href'])
            if mid is None:
                continue
            else:
                mid = mid.group(1)

            img = item.img['src']
            result.append({'mid': mid, 'img': img})
        return result

    def get_star_list(self, page=1):
        """
        分页获取演员列表
        :param page:页码
        :return: [{'sid':演员id,'name':演员姓名,'img':演员头像URL}],下一页的页码
                下一页页码为None则表示遍历结束
        """

        url_actresses = 'http://%s/actresses/currentPage/{page}' % self.server
        # 演员列表
        ptn_actresses = u'http://%s/star/(.*)' % self.ptn_server
        ptn_next_actresses = u'<a href="/cn/actresses/currentPage/(\d+)">下一页</a>'

        res = self.do_get(url_actresses.format(page=page))  # 这里好像不加headers也可以
        log('[HTTP: 200] got stars at page:%d' % page)

        result = []
        soup = BeautifulSoup(res)
        items = soup.find_all('div', class_='item')
        for item in items:
            sid = re.search(ptn_actresses, item.a['href'])
            if sid is None:
                continue
            else:
                sid = sid.group(1)

            img = item.img['src']
            name = item.span.string
            if name:
                name = name.encode('utf-8')
            result.append({'sid': sid, 'name': name, 'img': img})

        match = re.search(ptn_next_actresses, res)
        if match:
            return result, match.group(1)
        else:
            return result, None

    def get_movie_list(self, sid, page=1):
        """
        分页获取指定演员的所有作品
        :param sid: 演员id
        :param page: 页码
        :return: [{'mid':作品id,'img':作品封面URL}]，下一页的页码
                下一页页码为None则表示遍历结束
        """
        # url
        url_movie_list = 'http://%s/star/{sid}/currentPage/{page}' % self.server
        url_movie_list_home = 'http://%s/star/{sid}' % self.server

        # 作品列表
        ptn_movies = u'http://%s/movie/(.*)' % self.ptn_server
        ptn_next_movies = u'<a href="/cn/star/{sid}/currentPage/(\d+)">下一页</a>'

        # http referer
        referer_movie_list_home = 'http://%s/actresses' % self.server
        referer_movie_list = 'http://%s/star/{sid}/currentPage/{page}' % self.server

        headers = self.headers.copy()
        if page == 1:
            headers['Referer'] = referer_movie_list_home
            url = url_movie_list_home.format(sid=sid)
        else:
            headers['Referer'] = referer_movie_list.format(sid=sid, page=int(page) - 1)
            url = url_movie_list.format(sid=sid, page=page)

        res = self.do_get(url, headers=headers)
        log('[HTTP: 200] got movies of sid:%s at page:%s' % (sid, page))

        result = []
        soup = BeautifulSoup(res)
        items = soup.find_all('div', class_='item')
        if page == 1:
            items = items[1:]

        for item in items:
            mid = re.search(ptn_movies, item.a['href'])
            if mid is None:
                continue
            else:
                mid = mid.group(1)

            img = item.img['src']
            result.append({'mid': mid, 'img': img})

        match = re.search(ptn_next_movies.format(sid=sid), res)
        if match:
            return result, match.group(1)
        else:
            return result, None

    def get_movie(self, mid):
        """
        获取指定影片的信息
        :param mid: 影片id
        :return: {'mid':影片id,'id': 番号, 'name': 名称, 'time': 发行时间, 'length': 时长（分钟）,
                'cover': 封面大图URL, 'cates': 分类列表, 'actors': 演员列表, 'samples': 样图列表}
        """

        # url
        url_movie = 'http://%s/movie/{mid}' % self.server

        # 作品详情
        ptn_movie_name = u'<h3>(.*?)</h3>'
        ptn_movie_vid = u'<span class="header">识别码:</span> <span style="color:#CC0000;">(.*?)</span>'
        ptn_movie_time = u'<p><span class="header">发行时间:</span>(.*?)</p>'
        ptn_movie_length = u'<p><span class="header">长度:</span> (\d+)分钟</p>'
        ptn_movie_cover = u'<a class="bigImage" href=".*?"><img src="(.*?)".*?></a>'
        ptn_movie_cate = u'<a href="http://%s/genre/.*?">(.*?)</a>' % self.ptn_server
        ptn_movie_actor = u'<a class="avatar-box.*?" href="http://%s/star/(.*?)">' % self.ptn_server
        ptn_movie_sample = u'<a class="sample-box.*?" href="(.*?)">'

        res = self.do_get(url_movie.format(mid=mid))
        log('[HTTP: 200] got movie mid:%s' % mid)

        name = re.search(ptn_movie_name, res)
        name = name.group(1).strip() if name else ''

        _id = re.search(ptn_movie_vid, res)
        _id = _id.group(1) if _id else ''

        time = re.search(ptn_movie_time, res)
        time = time.group(1).strip() if time else ''

        length = re.search(ptn_movie_length, res)
        length = length.group(1).strip() if length else 0

        cover = re.search(ptn_movie_cover, res)
        cover = cover.group(1) if cover else ''

        cates = re.findall(ptn_movie_cate, res)
        actors = re.findall(ptn_movie_actor, res)
        samples = re.findall(ptn_movie_sample, res)

        return {'mid': mid, 'id': _id, 'name': name, 'time': time, 'length': length,
                'cover': cover, 'cates': cates, 'actors': actors, 'samples': samples}

    def search_movie(self, id_):
        url = 'http://%s/search/%s' % (self.server, id_)
        ptn = '<img src="(.*?)".*?>'
        res = self.do_get(url)

        if res.find(u'搜寻没有结果') > -1:
            return None
        else:
            m = re.search(ptn, res)
            if not m:
                log('[HTTP: 404] Search Fail movie:%s' % id_)
                return None
            else:
                log('[HTTP: 200] Search OK movie:%s' % id_)
                return m.group(1)

    def fetch_star(self, page):
        """
        获取指定页面的演员列表
        :param page:
        :return: 演员列表
        """
        stars, p = self.get_star_list(page)
        return stars

    def fetch_movie_id(self, sid):
        """
        获取指定演员的影片列表
        :param sid: 演员id
        :return: 影片列表
        """
        movie_list = []
        page = 1
        while page is not None:
            movies, page = self.get_movie_list(sid, page)
            movie_list += movies
        return movie_list


class Proxy(object):
    proxies = []

    @staticmethod
    def get_proxies_from_pachong_org():
        """
        从pachong.org 获取Proxy
        """
        proxies = []

        urls = ['http://pachong.org/transparent.html',
                'http://pachong.org/high.html',
                'http://pachong.org/anonymous.html'
                ]
        for url in urls:
            res = requests.get(url).text

            # var duck=1159+2359
            m = re.search('var ([a-zA-Z]+)=(.*?);', res)
            var = {m.group(1): eval(m.group(2))}

            # var bee=6474+1151^duck;
            exprs = re.findall('var ([a-zA-Z]+)=(\d+)\+(\d+)\^([a-zA-Z]+);', res)

            for expr in exprs:
                var[expr[0]] = int(expr[1]) + int(expr[2]) ^ var[expr[3]]

            soup = BeautifulSoup(res)
            table = soup.find('table', class_='tb')

            for tr in table.find_all('tr'):
                data = tr.find_all('td')
                ip = data[1].text

                if not re.match('\d+\.\d+\.\d+\.\d+', ip):
                    continue

                # (15824^seal)+1327
                script = data[2].script.text
                expr = re.search('\((\d+)\^([a-zA-Z]+)\)\+(\d+)', script)

                port = (int(expr.group(1)) ^ var[expr.group(2)]) + int(expr.group(3))
                proxies.append('%s:%s' % (ip, port))
        proxies = list(set(proxies))
        return proxies

    @staticmethod
    def get_proxies_from_cn_proxy():
        """
        从 cn-proxy.com 获取Proxy
        :return:
        """
        urls = [
            'http://cn-proxy.com/archives/218',
            'http://cn-proxy.com/'
        ]
        proxies = []

        for url in urls:
            res = requests.get(url).text
            data = re.findall('<td>(\d+\.\d+\.\d+\.\d+)</td>.*?<td>(\d+)</td>', res, re.DOTALL)

            for item in data:
                proxies.append('%s:%s' % (item[0], item[1]))
        return proxies

    @staticmethod
    def test_proxies(proxies, url, timeout):
        """
        测试代理。剔除响应时间大于timeout的代理
        :param proxies:  代理列表
        :param url:  测试链接
        :param timeout: 响应时间(s)
        :return:
        """

        def handler(signum, frame):
            raise requests.exceptions.Timeout()

        errors = []
        for proxy in proxies:
            try:
                signal.signal(signal.SIGALRM, handler)
                signal.alarm(timeout)
                start = time()
                res = requests.get(url, proxies={'http': proxy})
                end = time()
            except (requests.exceptions.ConnectionError, socket.error):
                log('[Proxy: %s] ConnectionError' % proxy)
                errors.append(proxy)
            except requests.exceptions.Timeout:
                log('[Proxy: %s] ConnectTimeout' % proxy)
                errors.append(proxy)
            else:
                if res.status_code != 200:
                    log('[HTTP: %d  ERROR]' % res.status_code)
                else:
                    escape = end - start
                    log('[Proxy: %s] Time:%f Length:%d' % (proxy, escape, len(res.text)))
            finally:
                signal.alarm(0)
        map(proxies.remove, errors)
        log('[HTTP Proxies] Available:%d Deprecated:%d' % (len(proxies), len(errors)))


class Agent(object):
    agents = [
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
        'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0',
        'Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0',
        'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
        'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
        'Opera/12.02 (Android 4.1; Linux; Opera Mobi/ADR-1111101157; U; en-US) Presto/2.9.201 Version/12.02',
        'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
        'Mozilla/5.0 (Linux; U; Android 2.3.3; de-ch; HTC Desire Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
        'Opera/9.25 (Windows NT 5.1; U; en)',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
        'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
        'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
        'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
        'Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7',
        'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0'
    ]

    @staticmethod
    def get_agent():
        return random.choice(Agent.agents)


class DBManager(object):
    def __init__(self, user, passwd, db):
        # 数据库
        self.dbconn = MySQLdb.connect(user=user, passwd=passwd, db=db,
                                      charset='utf8', use_unicode=True)

    def create_db(self):
        """
        创建数据库
        :return:
        """
        cursor = self.dbconn.cursor()
        cursor.execute('DROP TABLE IF EXISTS movie_actor')
        cursor.execute('DROP TABLE IF EXISTS movie_sample')
        cursor.execute('DROP TABLE IF EXISTS movie_cate')
        cursor.execute('DROP TABLE IF EXISTS movie')
        cursor.execute('DROP TABLE IF EXISTS star')

        cursor.execute("""
            CREATE TABLE movie(
                mid     VARCHAR(127) PRIMARY KEY ,
                id      VARCHAR(127),
                name    VARCHAR(127),
                time    VARCHAR(127),
                length  VARCHAR(127),
                cover   VARCHAR(127),
                small   VARCHAR(127)
            )
        """)
        cursor.execute("""
            CREATE TABLE star(
                sid     VARCHAR(127) PRIMARY KEY ,
                name    VARCHAR(127),
                img     VARCHAR(127)
            )
        """)
        cursor.execute("""
            CREATE TABLE movie_actor(
                mid_id     VARCHAR(127),
                sid_id     VARCHAR(127),
                PRIMARY KEY (mid_id,sid_id),
                FOREIGN KEY (mid_id) REFERENCES movie(mid),
                FOREIGN KEY (sid_id) REFERENCES star(sid)
            )
        """)
        cursor.execute("""
            CREATE TABLE movie_sample(
                mid_id     VARCHAR(127),
                img        VARCHAR(127),
                PRIMARY KEY (mid_id,img),
                FOREIGN KEY (mid_id) REFERENCES movie(mid)
            )
        """)
        cursor.execute("""
            CREATE TABLE movie_cate(
                mid_id     VARCHAR(127),
                cate    VARCHAR(127),
                PRIMARY KEY (mid_id,cate),
                FOREIGN KEY (mid_id) REFERENCES movie(mid)
            )
        """)

    def execute(self, sql, data):
        """
        存数据
        :param sql: sql字符串（包含占位符）
        :param data:  与占位符对应的数据
        :return:
        """
        cursor = self.dbconn.cursor()
        try:
            cursor.execute(sql, data)
        except _mysql_exceptions.IntegrityError:
            log('[MySQL: IntegrityError]')
        finally:
            self.dbconn.commit()
            cursor.close()

    def insert_star(self, star):
        """
        存演员
        :param star: 演员信息 dict
        :return:
        """
        sql = 'INSERT INTO star(sid,name,img) VALUES(%s,%s,%s)'
        self.execute(sql, (star['sid'], star['name'], star['img']))

    def insert_movie_id(self, movie):
        """
        存电影mid,缩略图small
        :param mid:
        :return:
        """
        sql = 'INSERT INTO movie(mid,small) VALUES(%s,%s)'
        self.execute(sql, (movie['mid'], movie['img']))

    def insert_movie_detail(self, movie, update=False):
        """
        存电影（更新）
        :param update:
        :param movie: 电影信息dict
        :return:
        """
        if update:
            sql = 'UPDATE movie SET id=%s,name=%s,time=%s,length=%s,cover=%s WHERE mid=%s'
            self.execute(sql,
                         (movie['id'], movie['name'], movie['time'],
                          movie['length'], movie['cover'], movie['mid']))
        else:
            sql = 'INSERT INTO movie(mid,small,id,name,time,length,cover) VALUES(%s,%s,%s,%s,%s,%s,%s)'
            self.execute(sql, (movie['mid'], movie['small'], movie['id'],
                               movie['name'], movie['time'],
                               movie['length'], movie['cover']))

    def insert_movie_actor(self, mid, sid):
        """
        存影片演员表
        :param mid: 电影id
        :param sid: 演员id
        :return:
        """
        sql = 'INSERT INTO movie_actor(mid_id,sid_id) VALUES(%s,%s)'
        self.execute(sql, (mid, sid))

    def insert_movie_sample(self, mid, img):
        """
        存电影样图
        :param mid: 电影id
        :param img: 图片url
        :return:
        """
        sql = 'INSERT INTO movie_sample(mid_id,img) VALUES(%s,%s)'
        self.execute(sql, (mid, img))

    def insert_movie_cate(self, mid, cate):
        """
        存电影分类
        :param mid: 电影id
        :param cate: 电影分类
        :return:
        """
        sql = 'INSERT INTO movie_cate(mid_id,cate) VALUES(%s,%s)'
        self.execute(sql, (mid, cate))

    def store_stars(self, stars):
        """
        存一批演员
        :param stars:
        :return:
        """
        map(self.insert_star, stars)

    def store_movie_id(self, movies):
        """
        存一批电影id
        :param movies:
        :return:
        """
        map(self.insert_movie_id, movies)

    def store_movie_detail(self, movie):
        """
        存电影详情
        :param movie: 电影信息
        :return:
        """
        self.insert_movie_detail(movie)

        mid = movie['mid']
        for actor in movie['actors']:
            self.insert_movie_actor(mid, actor)
        for cate in movie['cates']:
            self.insert_movie_cate(mid, cate)
        for sample in movie['samples']:
            self.insert_movie_sample(mid, sample)

    def update_movie_detail(self, movie):
        self.insert_movie_detail(movie, update=True)

        mid = movie['mid']
        for actor in movie['actors']:
            self.insert_movie_actor(mid, actor)
        for cate in movie['cates']:
            self.insert_movie_cate(mid, cate)
        for sample in movie['samples']:
            self.insert_movie_sample(mid, sample)

    def read_star(self):
        """
        读取所有演员信息
        :return:
        """
        cursor = self.dbconn.cursor()
        cursor.execute('SELECT sid FROM star')

        stars = []
        for (sid,) in cursor:
            stars.append(sid)
        cursor.close()
        return stars

    def read_movie_id(self):
        """
        读取所有电影id
        :return:
        """
        cursor = self.dbconn.cursor()
        cursor.execute('SELECT mid FROM movie')
        movies = []
        for (mid,) in cursor:
            movies.append(mid)
        cursor.close()
        return movies

    def has_movie(self, mid):
        cursor = self.dbconn.cursor()
        cursor.execute('SELECT * FROM movie WHERE mid=%s', mid)
        count = cursor.rowcount
        cursor.close()

        if count == 0:
            return False
        else:
            return True

    def get_max_mid(self):
        cursor = self.dbconn.cursor()
        cursor.execute('SELECT MAX(mid) FROM movie WHERE mid LIKE "____"')
        (mid,) = cursor.fetchone()
        cursor.close()

        return mid


processes = cpu_count() * 4


def down_star(spider, dbmanager, actor_pages):
    """
    在线获取演员列表，存入数据库
    :return:
    """
    pool = Pool(processes=processes)
    for page in range(1, actor_pages + 1):
        pool.apply_async(spider.fetch_star, args=(page,), callback=dbmanager.store_stars)
    pool.close()
    pool.join()
    log('Successfully down star')


def down_movie_id(spider, dbmanager):
    """
    读取数据库中的演员id,在线获取演员作品，存入数据库
    :return:
    """
    pool = Pool(processes=processes)
    for sid in dbmanager.read_star():
        pool.apply_async(spider.fetch_movie_id, args=(sid,), callback=dbmanager.store_movie_id)
    pool.close()
    pool.join()
    log('Successfully down movie id')


def down_movie_detail(spider, dbmanager):
    """
    读取数据库中的电影id，在线获取详情，存入数据库
    :return:
    """
    pool = Pool(processes=processes)
    for mid in dbmanager.read_movie_id():
        pool.apply_async(spider.get_movie, args=(mid,), callback=dbmanager.update_movie_detail)
    pool.close()
    pool.join()
    log('Successfully down movie detail')


if __name__ == '__main__':
    db = DBManager(user='root', passwd='19961020', db='avmoo')
    db.create_db()

    proxies = Proxy.get_proxies_from_cn_proxy()
    Proxy.test_proxies(proxies, 'http://www.avmoo.net/cn', 2)
    spider = Spider('www.avmoo.net', proxies)

    # 分三个步骤进行，不需要一次执行成功。
    down_star(spider, db, 182)  # 演员总页数
    # down_movie_id(spider, db)
    # down_movie_detail(spider, db)
