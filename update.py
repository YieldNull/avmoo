# coding:utf-8

"""
Update latest released movie.

Run update.py at 4am everyday using cron.

0 4 * * * /path_to_it/update.py > update_log.txt
"""

from avmoo import Spider, DBManager, Proxy


def mid2int(mid):
    """
    将mid转换为36进制，1-9a-z
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
    if value < 36:
        return str(value) if value < 10 else chr(value - 10 + 97)
    else:
        c = value % 36
        sub = (value - c) / 36
        c = str(c) if c < 10 else chr(c - 10 + 97)
        return '%s%s' % (int2mid(sub), c)


def fetch_missing(mid_max=None):
    proxies = Proxy.get_proxies_from_cn_proxy()
    Proxy.test_proxies(proxies, 'http://www.avmoo.net/cn/movie/5dlm', 2)

    spider = Spider('www.avmoo.net', proxies)
    db = DBManager('root', '19961020', 'avmoo')

    if mid_max is None:
        mid_max = spider.get_latest(1)[0]['mid']

    mid_list = db.read_movie_id()
    mid_list = map(mid2int, mid_list)
    mid_max = mid2int(mid_max)
    missing = set(range(1, mid_max + 1)) - set(mid_list)

    for mid_int in missing:
        mid = int2mid(mid_int)
        movie = spider.get_movie(mid)
        movie['small'] = spider.search_movie(movie['id']) or ''
        db.store_movie_detail(movie)


if __name__ == '__main__':
    fetch_missing()
