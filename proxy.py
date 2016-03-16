#!/usr/bin/env python3

import re
import requests
import signal
from time import time, sleep

from bs4 import BeautifulSoup

import socks
import socket

socks.set_default_proxy(socks.SOCKS5, "127.0.0.1", 1081, True)
socket.socket = socks.socksocket

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Accept': 'text / html, application / xhtml + xml, application / xml;'
              'q = 0.9, image / webp, * / *;q = 0.8',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Ubuntu Chromium/48.0.2564.116 Chrome/48.0.2564.116 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2',
}


def log(msg):
    print(msg)


def http(url, data=None, session=None):
    try:
        if data is None:
            res = requests.get(url, headers=headers) if session is None \
                else session.get(url, headers=headers)
        else:
            res = requests.post(url, headers=headers, data=data) if session is None \
                else session.post(url, headers=headers, data=data)

        code = res.status_code
        log('[{:d}] {:s} {:s}'.format(code, 'POST' if data is not None else 'GET', url))

        return res.text if code == 200 else ''
    except Exception as e:
        log(str(e))
        return ''


def from_pachong_org():
    """
    From "http://pachong.org/"
    :return:
    """
    proxies = []

    urls = ['http://pachong.org/transparent.html',
            'http://pachong.org/high.html',
            'http://pachong.org/anonymous.html'
            ]
    for url in urls:
        res = http(url)

        # var duck=1159+2359
        m = re.search('var ([a-zA-Z]+)=(.*?);', res)
        if not m:
            return []

        var = {m.group(1): eval(m.group(2))}

        # var bee=6474+1151^duck;
        exprs = re.findall('var ([a-zA-Z]+)=(\d+)\+(\d+)\^([a-zA-Z]+);', res)

        for expr in exprs:
            var[expr[0]] = int(expr[1]) + int(expr[2]) ^ var[expr[3]]

        soup = BeautifulSoup(res, 'lxml')
        table = soup.find('table', class_='tb')

        for tr in table.find_all('tr'):
            data = tr.find_all('td')
            ip = data[1].text

            if not re.match('\d+\.\d+\.\d+\.\d+', ip):
                continue

            # port=(15824^seal)+1327
            script = data[2].script.text
            expr = re.search('\((\d+)\^([a-zA-Z]+)\)\+(\d+)', script)

            port = (int(expr.group(1)) ^ var[expr.group(2)]) + int(expr.group(3))
            proxies.append('%s:%s' % (ip, port))
    proxies = list(set(proxies))
    return proxies


def from_cn_proxy():
    """
    From "http://cn-proxy.com/"
    :return:
    """
    urls = [
        'http://cn-proxy.com/archives/218',
        'http://cn-proxy.com/'
    ]
    proxies = []

    for url in urls:
        res = http(url)
        data = re.findall('<td>(\d+\.\d+\.\d+\.\d+)</td>.*?<td>(\d+)</td>', res, re.DOTALL)

        for item in data:
            proxies.append('%s:%s' % (item[0], item[1]))
    return proxies


def from_proxy_spy():
    """
    From "http://txt.proxyspy.net/proxy.txt"
    :return:
    """
    url = 'http://txt.proxyspy.net/proxy.txt'
    res = http(url)
    proxies = re.findall('(\d+\.\d+\.\d+\.\d+:\d+) .*', res)
    return proxies


def from_xici_daili():
    """
    From "http://www.xicidaili.com/"
    :return:
    """
    urls = [
        'http://www.xicidaili.com/nt/1',
        'http://www.xicidaili.com/nt/2',
        'http://www.xicidaili.com/nn/1',
        'http://www.xicidaili.com/nn/2',
        'http://www.xicidaili.com/wn/1',
        'http://www.xicidaili.com/wn/2',
        'http://www.xicidaili.com/wt/1',
        'http://www.xicidaili.com/wt/2'
    ]

    proxies = []
    for url in urls:
        res = http(url)
        data = re.findall('<td>(\d+\.\d+\.\d+\.\d+)</td>.*?<td>(\d+)</td>', res, re.DOTALL)
        proxies += ['{:s}:{:s}'.format(host, port) for (host, port) in data]
    return proxies


def from_get_proxy():
    """
    From "http://www.getproxy.jp"
    :return:
    """
    base = 'http://www.getproxy.jp/proxyapi?' \
           'ApiKey=659eb61dd7a5fc509bef01f2e8b15669dfdb0f54' \
           '&area={:s}&sort=requesttime&orderby=asc&page={:d}'

    urls = [base.format('CN', i) for i in range(1, 20)]
    urls += [base.format('US', i) for i in range(1, 20)]
    proxies = []

    for i in range(len(urls)):
        sleep(10)
        res = http(urls[i])
        soup = BeautifulSoup(res, 'lxml')
        proxies += [proxy.text for proxy in soup.find_all('ip')]
    return proxies


def from_hide_my_ip():
    """
    From "https://www.hide-my-ip.com/proxylist.shtml"
    :return:
    """
    url = 'https://www.hide-my-ip.com/proxylist.shtml'
    res = http(url)

    data = re.findall('"i":"(\d+\.\d+\.\d+\.\d+)","p":"(\d+)"', res)
    proxies = ['{:s}:{:s}'.format(host, port) for (host, port) in data]
    return proxies


def from_cyber_syndrome():
    """
    From "http://www.cybersyndrome.net/"
    :return:
    """
    urls = [
        'http://www.cybersyndrome.net/pld.html',
        'http://www.cybersyndrome.net/pla.html'
    ]

    proxies = []
    for url in urls:
        res = http(url)
        proxies += re.findall('(\d+\.\d+\.\d+\.\d+:\d+)', res)
    return proxies


def from_free_proxy_list():
    """
    From "http://free-proxy-list.net/"
    :return:
    """
    urls = [
        'http://www.us-proxy.org/',
        'http://free-proxy-list.net/uk-proxy.html'
    ]
    proxies = []

    for url in urls:
        res = http(url)
        data = re.findall('<tr><td>(\d+\.\d+\.\d+\.\d+)</td><td>(\d+)</td>', res)
        proxies += ['{:s}:{:s}'.format(host, port) for (host, port) in data]
    return proxies


def from_gather_proxy():
    """
    From "http://www.gatherproxy.com"
    :return:
    """
    url_login = 'http://www.gatherproxy.com/subscribe/login'
    url_info = 'http://www.gatherproxy.com/subscribe/infos'
    url_download = 'http://www.gatherproxy.com/proxylist/downloadproxylist/?sid={:s}'

    session = requests.session()  # enable cookie

    # captcha like "Eight - 5"=?
    operand_map = {
        'Zero': 0, '0': 0,
        'One': 1, '1': 1,
        'Two': 2, '2': 2,
        'Three': 3, '3': 3,
        'Four': 4, '4': 4,
        'Five': 5, '5': 5,
        'Six': 6, '6': 6,
        'Seven': 7, '7': 7,
        'Eight': 8, '8': 8,
        'Nine': 9, '9': 9
    }
    operator_map = {
        'plus': '+', '+': '+',
        'multiplied': '*', 'X': '*',
        'minus': '-', '-': '-'
    }

    # get captcha
    res = http(url_login, session=session)
    m = re.search('Enter verify code: <span class="blue">(.*?) = </span>', res)
    if not m:
        return []

    calcu = m.group(1).strip()
    opers = calcu.split()
    if len(opers) != 3:
        return []

    operand1 = operand_map.get(opers[0])
    operator = operator_map.get(opers[1])
    operand2 = operand_map.get(opers[2])

    try:
        result = eval('{} {} {}'.format(operand1, operator, operand2))
    except:
        return []

    data = {
        'Username': 'jun-kai-xin@163.com',
        'Password': 'N}rS^>&3',
        'Captcha': result
    }

    # post to login and redirect to info page to get download `id`
    http(url_login, data=data, session=session)
    res = http(url_info, session=session)

    m = re.search('<p><a href="/proxylist/downloadproxylist/\?sid=(\d+)">Download', res)
    if m is None:
        return []

    data = {
        'ID': m.group(1),
        'C': '',
        'P': '',
        'T': '',
        'U': 90  # uptime
    }

    # post id to get proxy list
    res = http(url_download.format(m.group(1)), data=data, session=session)
    session.close()

    proxies = res.split('\n')  # split the txt file
    return proxies


if __name__ == '__main__':
    for proxy in from_gather_proxy():
        print(proxy)


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
            print('[Proxy: %s] ConnectionError' % proxy)
            errors.append(proxy)
        except requests.exceptions.Timeout:
            print('[Proxy: %s] ConnectTimeout' % proxy)
            errors.append(proxy)
        else:
            if res.status_code != 200:
                print('[HTTP: %d  ERROR]' % res.status_code)
            else:
                escape = end - start
                print('[Proxy: %s] Time:%f Length:%d' % (proxy, escape, len(res.text)))
        finally:
            signal.alarm(0)
    map(proxies.remove, errors)
    print('[HTTP Proxies] Available:%d Deprecated:%d' % (len(proxies), len(errors)))
