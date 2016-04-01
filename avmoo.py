#!/usr/bin/env python3

"""
利用爬取的代理IP，爬取avmoo.com的影片信息。存入mongodb。

Cron:

0 1 * * * /path/to/avmoo.py -l --site https://avmo.pw --col avmoo
"""

# 第一导入，因为proxy中使用了monkey_patch()
from proxy import fetch_proxies, test_proxies, log, Proxy, safe_http, enable_logger

from gevent.pool import Pool
import re
import pymongo
import random

from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient()
db = client.avmoo
collection = None

max_mid = '1'
home_url = 'https://www.avmoo.com/cn'


def mid2int(mid):
    """
    将36进制字符串mid转换为10进制，1-9a-z
    :param mid:
    :return:
    """
    value = 0
    mid = mid.lower()
    length = len(mid)

    for i in range(len(mid)):
        c = mid[i]
        factor = int(c) if c.isdigit() else ord(c) - 97 + 10
        value += 36 ** (length - i - 1) * factor
    return value


def int2mid(value):
    """
    将10进制mid转换为36进制字符串，1-9a-z
    :param value:
    :return:
    """
    if value < 36:
        return str(value) if value < 10 else chr(value - 10 + 97)
    else:
        c = value % 36
        sub = int((value - c) / 36)
        c = str(c) if c < 10 else chr(c - 10 + 97)
        return '%s%s' % (int2mid(sub), c)


def check_redirect(res):
    """
    查看是否有重定向，有则将home_url改为重定向后的url
    :param history:
    """
    global home_url
    if len(res.history) > 0:
        real_url = res.url
        m = re.search('(https://.*?/cn)/.*', real_url)
        print(real_url)
        if m is not None:
            home_url = m.group(1)


def safe_search(ptn, src, pair=False, integer=False):
    """
    search ptn in src, and return group search result
    :param ptn: 正则
    :param src: 数据源
    :param pair: 是否搜索一组数据，默认搜索一个
    :param integer: 是否搜索一个数字，默认字符串
    :return: 搜索的结果
    """
    m = re.search(ptn, src)
    if m:
        if pair:
            return m.group(1).strip(), m.group(2).strip()
        elif integer:
            return int(m.group(1))
        else:
            return m.group(1).strip()
    else:
        if pair:
            return '', ''
        elif integer:
            return -1
        else:
            return ''


def get_movie(url, source, code=200):
    """
    获取指定影片的全部信息
    参见 "https://www.avmoo.com/cn/movie/500"

    :param url: 网页url
    :param source: 网页源码
    :param code: 状态码。
    :return: code=200 则返回影片信息，code=404则返回{'mid':mid},其它情况返回None
    """
    mid = re.search('.*?/movie/(.*)', url).group(1)

    if code == 404:
        return {'mid': mid}
    elif code != 200:
        return None

    server = re.search('(.*?)/movie/.*', url).group(1)
    ptn_server = server.replace('.', '\.')

    ptn_name = '<h3>(.*?)</h3>'
    ptn_fid = '<span class="header">识别码:</span> <span.*?>(.*?)</span>'

    ptn_time = '<p><span class="header">发行时间:</span>(.*?)</p>'
    ptn_length = '<p><span class="header">长度:</span> (\d+)分钟</p>'
    ptn_cover = '<a class="bigImage" href=".*?"><img src="(.*?)".*?></a>'

    ptn_director = '<a href="{:s}/director/(.*?)">(.*?)</a>'.format(ptn_server)
    ptn_studio = '<a href="{:s}/studio/(.*?)">(.*?)</a>'.format(ptn_server)
    ptn_label = '<a href="{:s}/label/(.*?)">(.*?)</a>'.format(ptn_server)
    ptn_series = '<a href="{:s}/series/(.*?)">(.*?)</a>'.format(ptn_server)
    ptn_genre = '<a href="{:s}/genre/(.*?)">(.*?)</a>'.format(ptn_server)

    ptn_sample = '<a class="sample-box.*?" href="(.*?)">'
    ptn_star = '{:s}/star/(.*)'.format(ptn_server)

    name = safe_search(ptn_name, source)  # 片名
    if len(name) < 1:  # not 200
        return None

    fid = safe_search(ptn_fid, source)  # 番号
    time = safe_search(ptn_time, source)  # 发行时间
    length = safe_search(ptn_length, source, integer=True)  # 片长，单位分钟
    cover = safe_search(ptn_cover, source)  # 大图URL

    director = safe_search(ptn_director, source, pair=True)  # (导演id, 导演名)
    studio = safe_search(ptn_studio, source, pair=True)  # (制作商id, 制作商名)
    label = safe_search(ptn_label, source, pair=True)  # (发行商id, 发行商名)
    series = safe_search(ptn_series, source, pair=True)  # (系列id， 系列名)

    genres = re.findall(ptn_genre, source)  # [(类别id, 类别名)...]
    samples = re.findall(ptn_sample, source)  # [样图URL...]

    stars = []
    soup = BeautifulSoup(source, 'lxml')
    for star in soup.find_all(name='a', class_='avatar-box'):
        sid = safe_search(ptn_star, star['href'])
        name = star.span.text.strip()
        stars.append({'id': sid, 'name': name})

    document = {
        'mid': mid,
        'fid': fid,
        'name': name.replace(fid, '').strip(),
        'time': time,  # datetime.strptime(time, "%Y-%m-%d"),
        'length': length,
        'cover': cover,

        'director': {
            'id': director[0],
            'name': director[1],
        },
        'studio': {
            'id': studio[0],
            'name': studio[1],
        },
        'label': {
            'id': label[0],
            'name': label[1],
        },
        'series': {
            'id': series[0],
            'name': series[1],
        },

        'genres': [
            {'id': genre[0], 'name': genre[1]} for genre in genres
            ],

        'stars': stars,
        'samples': samples
    }

    return document


def get_latest():
    """
    分页获取最新的作品链接
    :param page:
    :return:
    """
    ptn_movie_href = '<a class="movie-box.*?" href=".*?/cn/movie/(.*?)">'
    source = safe_http(home_url)

    m = re.search(ptn_movie_href, source)
    return m.group(1) if m is not None else None


def store_movie(url, source, code=None):
    """
    将电影信息存入mongodb
    :param url: 电影url
    :param source: 对应的源码
    :param code: http状态码
    """

    document = get_movie(url, source, code)

    if document is not None:
        try:
            result = collection.insert_one(document)
            log('[Mongodb] store document {:s}'.format(str(result.inserted_id)))
        except pymongo.errors.DuplicateKeyError:
            log('[Mongodb] Already exists')


def query_proxies():
    """
    查询已存可使用的proxies
    :return: set
    """
    query = Proxy.select().where(~(Proxy.status_code >> None))
    return set([proxy.proxy for proxy in query])


def query_missing():
    """
    get还没有爬的movie列表
    :return: mid的10进制形式
    """
    cursor = collection.find(filter=None, projection={'mid': True, '_id': False})
    movies = [mid2int(document['mid']) for document in cursor]
    cursor.close()

    available = set(range(1, mid2int(max_mid) + 1))
    instore = set(movies)

    missing = list(available - instore)
    log('[Missing] {:d} items'.format(len(missing)))

    return missing


def fetch_when_test():
    """
    测试代理ip的同时进行爬取目标页面信息
    """
    missing = query_missing()
    urls = ['{:s}/movie/{:s}'.format(home_url, int2mid(i)) for i in missing]
    ps = fetch_proxies()
    test_proxies(ps, many_urls=urls, call_back=store_movie)


def fetch_using_store():
    """
    使用已有的代理ip来爬
    """
    proxies = query_proxies()
    bad_proxies = set()

    def job(url, proxy):
        res = safe_http(url,
                        proxies={
                            'https': 'https://{}'.format(proxy),
                            'http': 'http://{}'.format(proxy)
                        }, want_obj=True, timeout=15)
        if res is not None:
            check_redirect(res)
            store_movie(url, res.text, res.status_code)
        else:
            bad_proxies.add(proxy)

    missing = query_missing()

    pool = Pool(100)
    while len(missing) > 0:
        for proxy in proxies:
            if len(missing) == 0:
                break

            index = random.randint(0, len(missing) - 1)
            int_mid = missing[index]
            missing.pop(index)  # 从待爬取列表移除

            url = '{:s}/movie/{:s}'.format(home_url, int2mid(int_mid))
            pool.spawn(job, url, proxy)
        pool.join()

        # 清除不可用的proxy
        proxies -= bad_proxies
        log('[Proxy] Clear {:d} bad proxies. Available {:d}'.format(len(bad_proxies), len(proxies)))
        bad_proxies.clear()
        if len(proxies) < 80:
            proxies = query_proxies()  # 重查

        # 待爬列表空了之后，再查一遍没有存到数据库中的影片
        # 因为之前会遇到403或timeout等错误
        if len(missing) == 0:
            missing = query_missing()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='A spider for three specific porn sites')

    parser.add_argument('-l', dest='logging', action='store_true',
                        help='Use logger or print to stdout. Missing is to stdout')

    parser.add_argument('-t', dest='test', action='store_true',
                        help='Get new proxies and test them'
                             'and fetch missing movies in the meantime.'
                             'Default is using proxies in database')

    parser.add_argument('--mid', dest='mid', action='store', type=str,
                        help='The max mid among the movies on the site. Like "5f20"')

    parser.add_argument('--site', dest='site', action='store', required=True, type=str,
                        help='Home URL of the site. Like "https://avmo.pw')

    parser.add_argument('--col', dest='collection', action='store', required=True,
                        choices=["avmoo", "avsox", "avmemo"],
                        help='Mongodb collection name.')

    args = parser.parse_args()

    if args.logging:
        enable_logger()

    max_mid = args.mid if args.mid else get_latest()
    home_url = args.site + '/cn'

    if args.collection == 'avmoo':
        collection = db.avmoo
    elif args.collection == 'avmemo':
        collection = db.avmemo
    else:
        collection = db.avsox

    if args.test:
        fetch_when_test()
    else:
        fetch_using_store()

    log('job done')
