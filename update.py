# coding:utf-8
import re
import requests
import signal
from bs4 import BeautifulSoup
from time import time

proxies = []


def get_proxies():
    global proxies

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


def test_proxies(url, timeout):
    def handler(signum, frame):
        raise requests.exceptions.Timeout()

    errors = []
    for proxy in proxies:
        print 'test %s' % proxy
        try:
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(timeout)
            start = time()
            res = requests.get(url, proxies={'http': proxy})
            end = time()
        except requests.exceptions.ConnectionError:
            print 'proxy:%s ConnectionError' % proxy
            errors.append(proxy)
        except requests.exceptions.Timeout:
            print 'proxy:%s ConnectTimeout' % proxy
            errors.append(proxy)
        else:
            if res.status_code != 200:
                print '[%d HTTP ERROR]' % res.status_code
            else:
                escape = end - start
                print 'proxy:%s time:%f' % (proxy, escape), len(res.text)
        finally:
            signal.alarm(0)
    map(proxies.remove, errors)
    for proxy in proxies:
        print proxy
    print len(proxies), len(errors)


get_proxies()
test_proxies('http://www.avmoo.net/cn', 2)
