# coding:utf-8

"""
A spider for porn sites: www.javmoo.xyz, www.avless.com, www.avmemo.com
"""

import re
import random
import requests
import MySQLdb
import _mysql_exceptions
from time import time
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count, current_process

# http headers
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

# User-Agent Pool
user_agents = []
with open('agents.txt') as f:
    for line in f:
        user_agents.append(line.split())

# HTTP proxy
http_proxies = []
with open('proxies.txt') as f:
    for line in f:
        l = line.split()
        http_proxies.append('%s:%s' % (l[0], l[1]))

http_proxies = list(set(http_proxies))
http_proxies.append('localhost')  # 随机到localhost就不使用代理


def _log(msg):
    """
    日志前加入进程名称
    :param msg:
    :return:
    """
    name = current_process().name
    print '[%s] %s' % (name, msg)


def _do_get(url, hds=headers, use_proxy=True):
    """
    获取源码
    :param url: URL
    :param hds: HTTP 请求头
    :param use_proxy: 是否使用代理
    :return: 源码
    """
    agent = random.choice(user_agents)
    hds = hds.copy()
    hds['User-Agent'] = agent

    retry = 0
    res = ''
    while True:
        try:
            # 随机选取代理
            proxy = random.choice(http_proxies)
            proxies = {'http': proxy}

            if (proxy == 'localhost') or (not use_proxy):
                res = requests.get(url, headers=hds)
            else:
                res = requests.get(url, headers=hds, proxies=proxies)
        except requests.exceptions.Timeout:
            _log('[HTTP: Timeout]')
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.RequestException):
            _log('[HTTP: Exception]')
            continue

        # 判断返回码
        code = res.status_code
        if code != 200:
            _log('[HTTP: %d] retrying......' % code)
            if not use_proxy:  # 重试时用代理
                use_proxy = True

            # 重试五十则跳出循环，返回
            retry += 1
            if retry > 50:
                return ''
        else:
            return res.text  # 跳出循环，返回源码


class Spider(object):
    def __init__(self, domain):
        # 网站信息
        self.domain = domain
        self.server = domain + '/cn'
        self.ptn_server = self.server.replace('.', '\.')
        self.headers = headers.copy()
        self.headers['Host'] = domain

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

        res = _do_get(url_actresses.format(page=page))
        _log('[HTTP: 200] got stars at page:%d' % page)

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

        hd = headers.copy()
        if page == 1:
            hd['Referer'] = referer_movie_list_home
            url = url_movie_list_home.format(sid=sid)
        else:
            hd['Referer'] = referer_movie_list.format(sid=sid, page=int(page) - 1)
            url = url_movie_list.format(sid=sid, page=page)

        res = _do_get(url, hd)
        _log('[HTTP: 200] got movies of sid:%s at page:%s' % (sid, page))

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

        res = _do_get(url_movie.format(mid=mid), use_proxy=False)
        _log('[HTTP: 200] got movie mid:%s' % mid)

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


class Movie(object):
    def __init__(self, mid, _id=None, name=None, time=None,
                 length=None, cover=None, small=None):
        self.mid = mid
        self._id = _id
        self.name = name
        self.time = time
        self.length = length
        self.cover = cover
        self.small = small


class Star(object):
    def __init__(self, sid, name=None, img=None):
        self.sid = sid
        self.name = name
        self.img = img


class MovieActor(object):
    def __init__(self, mid, sid):
        self.mid = mid
        self.sid = sid


class MovieSample(object):
    def __init__(self, mid, img):
        self.mid = mid
        self.img = img


class MovieCate(object):
    def __init__(self, mid, cate):
        self.mid = mid
        self.cate = cate


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
                mid     VARCHAR(127),
                sid     VARCHAR(127),
                PRIMARY KEY (mid,sid),
                FOREIGN KEY (mid) REFERENCES movie(mid),
                FOREIGN KEY (sid) REFERENCES star(sid)
            )
        """)
        cursor.execute("""
            CREATE TABLE movie_sample(
                mid     VARCHAR(127),
                img     VARCHAR(127),
                PRIMARY KEY (mid,img),
                FOREIGN KEY (mid) REFERENCES movie(mid)
            )
        """)
        cursor.execute("""
            CREATE TABLE movie_cate(
                mid     VARCHAR(127),
                cate    VARCHAR(127),
                PRIMARY KEY (mid,cate),
                FOREIGN KEY (mid) REFERENCES movie(mid)
            )
        """)

    def insert(self, sql, data):
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
            _log('[MySQL: IntegrityError]')
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
        self.insert(sql, (star['sid'], star['name'], star['img']))

    def insert_movie_id(self, movie):
        """
        存电影mid,缩略图small
        :param mid:
        :return:
        """
        sql = 'INSERT INTO movie(mid,small) VALUES(%s,%s)'
        self.insert(sql, (movie['mid'], movie['img']))

    def insert_movie_detail(self, movie):
        """
        存电影（更新）
        :param movie: 电影信息dict
        :return:
        """
        sql = 'UPDATE movie SET id=%s,name=%s,time=%s,length=%s,cover=%s WHERE mid=%s'

        self.insert(sql,
                    (movie['id'], movie['name'], movie['time'],
                     movie['length'], movie['cover'], movie['mid']))

    def insert_movie_actor(self, mid, sid):
        """
        存影片演员表
        :param mid: 电影id
        :param sid: 演员id
        :return:
        """
        sql = 'INSERT INTO movie_actor(mid,sid) VALUES(%s,%s)'
        self.insert(sql, (mid, sid))

    def insert_movie_sample(self, mid, img):
        """
        存电影样图
        :param mid: 电影id
        :param img: 图片url
        :return:
        """
        sql = 'INSERT INTO movie_sample(mid,img) VALUES(%s,%s)'
        self.insert(sql, (mid, img))

    def insert_movie_cate(self, mid, cate):
        """
        存电影分类
        :param mid: 电影id
        :param cate: 电影分类
        :return:
        """
        sql = 'INSERT INTO movie_cate(mid,cate) VALUES(%s,%s)'
        self.insert(sql, (mid, cate))

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
        cursor.execute('SELECT mid FROM movie where mid > "31ps"')
        movies = []
        for (mid,) in cursor:
            movies.append(mid)
        cursor.close()
        return movies


processes = cpu_count()


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
    _log('Successfully down star')


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
    _log('Successfully down movie id')


def down_movie_detail(spider, dbmanager):
    """
    读取数据库中的电影id，在线获取详情，存入数据库
    :return:
    """
    pool = Pool(processes=processes)
    for mid in dbmanager.read_movie_id():
        pool.apply_async(spider.get_movie, args=(mid,), callback=dbmanager.store_movie_detail)
    pool.close()
    pool.join()
    _log('Successfully down movie detail')


def test_proxies(url):
    """
    测试代理相应速度
    :return:
    """
    errors = []
    for proxy in http_proxies:
        print 'test %s' % proxy
        try:
            start = time()
            res = requests.get(url, proxies={'http': proxy})
            end = time()
        except requests.exceptions.ConnectionError:
            print 'proxy:%s ConnectionError' % proxy
            errors.append(proxy)
        except requests.exceptions.ReadTimeout:
            print 'proxy:%s ConnectTimeout' % proxy
            errors.append(proxy)
        else:
            timeout = end - start
            print 'proxy:%s time:%f' % (proxy, timeout), len(res.text)

    map(http_proxies.remove, errors)
    for proxy in http_proxies:
        print proxy


if __name__ == '__main__':
    db = DBManager(user='root', passwd='19961020', db='avmoo')
    # db.create_db()

    spider = Spider(domain='www.javmoo.xyz')
    # down_star(spider, db, 182)
    # down_movie_id(spider, db)
    # down_movie_detail(spider, db)
