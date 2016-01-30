# coding:utf-8

"""
Copy javmoo.xyz
"""
import random
import re
from time import sleep
import requests
from bs4 import BeautifulSoup
import MySQLdb
import _mysql_exceptions
from multiprocessing import Pool, cpu_count, current_process

# URL以及正则pattern
host = 'www.javmoo.xyz'
server = host + '/cn'
ptn_server = server.replace('.', '\.')

# url
url_actresses = 'http://%s/actresses/currentPage/{page}' % server
url_movie_list = 'http://%s/star/{sid}/currentPage/{page}' % server
url_movie_list_home = 'http://%s/star/{sid}' % server
url_movie = 'http://%s/movie/{mid}' % server

# 演员列表
ptn_actresses = u'http://%s/star/(.*)' % ptn_server
ptn_next_actresses = u'<a href="/cn/actresses/currentPage/(\d+)">下一页</a>'

# 作品列表
ptn_movies = u'http://%s/movie/(.*)' % ptn_server
ptn_next_movies = u'<a href="/cn/star/{sid}/currentPage/(\d+)">下一页</a>'

# 作品详情
ptn_movie_name = u'<h3>(.*?)</h3>'
ptn_movie_vid = u'<span class="header">识别码:</span> <span style="color:#CC0000;">(.*?)</span>'
ptn_movie_time = u'<p><span class="header">发行时间:</span>(.*?)</p>'
ptn_movie_length = u'<p><span class="header">长度:</span> (\d+)分钟</p>'
ptn_movie_cover = u'<a class="bigImage" href=".*?"><img src="(.*?)".*?></a>'
ptn_movie_cate = u'<a href="http://%s/genre/.*?">(.*?)</a>' % ptn_server
ptn_movie_actor = u'<a class="avatar-box" href="http://%s/star/(.*?)">' % ptn_server
ptn_movie_sample = u'<a class="sample-box" href="(.*?)">'

# http headers
headers = {
    'Host': host,
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Accept': 'text / html, application / xhtml + xml, application / xml;'
              'q = 0.9, image / webp, * / *;q = 0.8',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Ubuntu Chromium/48.0.2564.82 Chrome/48.0.2564.82 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2',
}

# http referer
referer_movie_list_home = 'http://%s/actresses' % server
referer_movie_list = 'http://%s/star/{sid}/currentPage/{page}' % server
referer_movie = 'http://%s/star/{sid}' % server

delay = 0.5

# User-Agent Pool
user_agents = []
with open('agents.txt') as f:
    for line in f:
        user_agents.append(line.split())

http_proxies = []
with open('proxies.txt') as f:
    for line in f:
        l = line.split()
        http_proxies.append('%s:%s' % (l[0], l[1]))

# 数据库
dbconn = MySQLdb.connect(user='root', passwd='19961020', db='avmoo', charset='utf8', use_unicode=True)


def do_get(url, hds=headers, retry=0):
    """
    获取源码
    :param url:
    :param hds:
    :param retry:
    :return:
    """
    agent = random.choice(user_agents)
    hds = hds.copy()
    hds['User-Agent'] = agent

    try:
        if retry == 0:
            res = requests.get(url, headers=hds, timeout=3)
        elif retry <= 2:
            res = requests.get(url, headers=headers, timeout=3, proxies={'http': 'lotusland.club:3128'})
        else:
            proxy = random.choice(http_proxies)
            proxies = {'http': proxy}
            res = requests.get(url, headers=hds, timeout=3, proxies=proxies)
    except requests.exceptions.Timeout:
        return do_get(url, headers, retry + 1)

    code = res.status_code

    if code != 200:
        print '[%d] agent:%d retrying......' % (code, user_agents.index(agent))
        return do_get(url, headers, retry + 1)
    return res.text, code


def get_star_list(page=1):
    """
    分页获取演员列表
    :param page:页码
    :return: [{'sid':演员id,'name':演员姓名,'img':演员头像URL}],下一页的页码
            下一页页码为None则表示遍历结束
    """
    res, code = do_get(url_actresses.format(page=page))
    print '[%d] page:%d' % (code, page)

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


def get_movie_list(sid, page=1):
    """
    分页获取指定演员的所有作品
    :param sid: 演员id
    :param page: 页码
    :return: [{'mid':作品id,'img':作品封面URL}]，下一页的页码
            下一页页码为None则表示遍历结束
    """
    hd = headers.copy()
    if page == 1:
        hd['Referer'] = referer_movie_list_home
        url = url_movie_list_home.format(sid=sid)
    else:
        hd['Referer'] = referer_movie_list.format(sid=sid, page=int(page) - 1)
        url = url_movie_list.format(sid=sid, page=page)

    res, code = do_get(url, hd)
    print '[%d] sid:%s page:%s' % (code, sid, page)

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


def get_movie(mid):
    """
    获取指定影片的信息
    :param mid: 影片id
    :return: {'mid':影片id,'id': 番号, 'name': 名称, 'time': 发行时间, 'length': 时长（分钟）,
            'cover': 封面大图URL, 'cates': 分类列表, 'actors': 演员列表, 'samples': 样图列表}
    """
    sleep(delay)
    res, code = do_get(url_movie.format(mid=mid))
    print '[%d] mid:%s' % (code, mid)

    name = re.search(ptn_movie_name, res)
    name = name.group(1).strip() if name else ''

    id_ = re.search(ptn_movie_vid, res)
    id_ = id_.group(1) if id_ else ''

    time = re.search(ptn_movie_time, res)
    time = time.group(1).strip() if time else ''

    length = re.search(ptn_movie_length, res)
    length = length.group(1).strip() if length else 0

    cover = re.search(ptn_movie_cover, res)
    cover = cover.group(1) if cover else ''

    cates = re.findall(ptn_movie_cate, res)
    actors = re.findall(ptn_movie_actor, res)
    samples = re.findall(ptn_movie_sample, res)

    return {'mid': mid, 'id': id_, 'name': name, 'time': time, 'length': length,
            'cover': cover, 'cates': cates, 'actors': actors, 'samples': samples}


def fetch_star(page):
    """
    获取指定页面的演员列表
    :param page:
    :return:
    """
    try:
        stars, p = get_star_list(page)
    except requests.exceptions.ConnectionError:
        print 'ConnectionError: fetch_star page:%d' % page
        return fetch_star(page)
    else:
        leng = len(stars)
        if leng == 0:
            return fetch_star(page)
        print page, len(stars)
        return stars


def fetch_movie_id(sid):
    """
    获取指定演员的影片列表
    :param sid:
    :return:
    """
    movie_list = []
    page = 1
    while page is not None:
        try:
            movies, page = get_movie_list(sid, page)
        except requests.exceptions.ConnectionError:
            print 'ConnectionError: fetch_movie_id sid:%s' % sid
            continue
        movie_list += movies
        leng = len(movies)
        if leng == 0:
            page -= 1
            continue
        print sid, len(movies)
    return movie_list


def fetch_movie_detail(mid):
    """
    获取电影详情
    :param: mid
    :return:
    """
    try:
        movie = get_movie(mid)
    except requests.exceptions.ConnectionError:
        print 'ConnectionError: mid:%s' % mid
        return fetch_movie_detail(mid)
    else:
        print mid
        return movie


processes = cpu_count() * 4


def down_star():
    pool = Pool(processes=processes)
    for page in range(1, 183):
        pool.apply_async(fetch_star, args=(page,), callback=store_stars)
    pool.close()
    pool.join()
    print 'successfully down star'


def down_movie_id():
    pool = Pool(processes=processes)
    for sid in read_star():
        pool.apply_async(fetch_movie_id, args=(sid,), callback=store_movie_id)
    pool.close()
    pool.join()
    print 'successfully down movie id'


def down_movie_detail():
    pool = Pool(processes=processes)
    for mid in read_movie_id():
        pool.apply_async(fetch_movie_detail, args=(mid,), callback=store_movie_detail)
    pool.close()
    pool.join()
    print 'successfully down movie detail'


def create_db():
    """
    创建数据库
    :return:
    """
    cursor = dbconn.cursor()
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


def insert(sql, data):
    """
    存数据
    :param sql: sql字符串（包含占位符）
    :param data:  与占位符对应的数据
    :return:
    """
    cursor = dbconn.cursor()
    try:
        cursor.execute(sql, data)
    except _mysql_exceptions.IntegrityError:
        print 'duplicated'
    finally:
        dbconn.commit()
        cursor.close()


def insert_star(star):
    """
    存演员
    :param star: 演员信息 dict
    :return:
    """
    sql = 'INSERT INTO star(sid,name,img) VALUES(%s,%s,%s)'
    insert(sql, (star['sid'], star['name'], star['img']))


def insert_movie_id(movie):
    """
    存电影mid,缩略图small
    :param mid:
    :return:
    """
    sql = 'INSERT INTO movie(mid,small) VALUES(%s,%s)'
    insert(sql, (movie['mid'], movie['img']))


def insert_movie_detail(movie):
    """
    存电影（更新）
    :param movie: 电影信息dict
    :return:
    """
    sql = 'UPDATE movie SET id=%s,name=%s,time=%s,length=%s,cover=%s WHERE mid=%s'

    insert(sql,
           (movie['id'], movie['name'], movie['time'],
            movie['length'], movie['cover'], movie['mid']))


def insert_movie_actor(mid, sid):
    """
    存影片演员表
    :param mid: 电影id
    :param sid: 演员id
    :return:
    """
    sql = 'INSERT INTO movie_actor(mid,sid) VALUES(%s,%s)'
    insert(sql, (mid, sid))


def insert_movie_sample(mid, img):
    """
    存电影样图
    :param mid: 电影id
    :param img: 图片url
    :return:
    """
    sql = 'INSERT INTO movie_sample(mid,img) VALUES(%s,%s)'
    insert(sql, (mid, img))


def insert_movie_cate(mid, cate):
    """
    存电影分类
    :param mid: 电影id
    :param cate: 电影分类
    :return:
    """
    sql = 'INSERT INTO movie_cate(mid,cate) VALUES(%s,%s)'
    insert(sql, (mid, cate))


def store_stars(stars):
    """
    存一批演员
    :param stars:
    :return:
    """
    map(insert_star, stars)


def store_movie_id(movies):
    """
    存一批电影
    :param movies:
    :return:
    """
    map(insert_movie_id, movies)


def store_movie_detail(movie):
    insert_movie_detail(movie)

    mid = movie['mid']
    for actor in movie['actors']:
        insert_movie_actor(mid, actor)
    for cate in movie['cates']:
        insert_movie_cate(mid, cate)
    for sample in movie['samples']:
        insert_movie_sample(mid, sample)


def read_star():
    cursor = dbconn.cursor()
    cursor.execute('SELECT sid FROM star')

    stars = []
    for (sid,) in cursor:
        stars.append(sid)
    cursor.close()
    return stars


def read_movie_id():
    cursor = dbconn.cursor()
    cursor.execute('SELECT mid FROM movie')
    movies = []
    for (mid,) in cursor:
        movies.append(mid)
    cursor.close()
    return movies


def serial():
    for sid in read_star():
        ml = fetch_movie_id(sid)
        store_movie_id(ml)


def test():
    for sid in read_star():
        page = 1
        while page:
            movie_list, page = get_movie_list(sid, page)
            # for movie in movie_list:
            #     get_movie(movie['mid'])


if __name__ == '__main__':
    create_db()
    down_star()
    down_movie_id()
    down_movie_detail()
