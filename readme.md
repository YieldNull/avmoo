A spider for porn sites: [www.avmoo.net](http://www.avmoo.net), [www.avless.net](http://www.avless.net), [www.avmemo.net](http://www.avmemo.net)

### 爬虫

采用代理IP以及`User-Agent`池来规避反爬虫机制。

先爬取所有演员的信息，然后再爬取每个演员对应的电影链接(mid)，最后根据电影链接爬取电影详情。

三个步骤不需要一次连续执行成功。失败后可以根据数据库中已经存有的数据再次启动，在`DBManager`的`read*`函数中加入`where`排除掉已经爬取的数据即可。

采用`multiprocessing`进行并发。

不知道是网络阻塞还是`multiprocessing`的库有问题，爬取一段时间后，会出现假死（所有工作进程都不工作了，然而并没有报错），此时需要`Ctrl+C`手动重启（进程池重新分配进程）。


### 用法

``` python
db = DBManager(user='root', passwd='19961020', db='avmoo')
db.create_db()

proxies = Proxy.get_proxies_from_cn_proxy()
Proxy.test_proxies(proxies, 'http://www.avmoo.net/cn', 2)
spider = Spider('www.avmoo.net', proxies)

# 分三个步骤进行，不需要一次执行成功。
down_star(spider, db, 182)  # 演员总页数
# down_movie_id(spider, db)
# down_movie_detail(spider, db)
```

### 更新&查漏

电影的mid是按36进制依次排列的，只要获取到最新的电影mid，与数据库的mid进行比对，就知道缺少了那些电影。

然后发现居然少爬了一半，**很多电影都没有演员信息**，因此遗漏了很多。

原则上每天运行`update.py`就能同步新发布的电影，然后每周爬取一下新的演员就ok了。

### 弃用

由于有较多的影片遗漏，而采用多进程会假死，需要人工维护（虽然可以写脚本定时kill与重启），于是弃用此方法。

为了避免遗漏，可以直接通过电影mid进入详情页进行爬取，详情页有除了小封面的所有信息。至于电影小封面，可以通过搜索番号进行爬取。

尝试一下用`Scrapy`或者`greenlet`解决并发问题

### 镜像站点

用爬取的数据建立镜像站点：[www.fuckthedog.cc](http://www.fuckthedog.cc)

数据库模型如下，为了简单起见，每个字段都是`VARCHAR(127)`

```python
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
        cate       VARCHAR(127),
        PRIMARY KEY (mid_id,cate),
        FOREIGN KEY (mid_id) REFERENCES movie(mid)
    )
""")
```

