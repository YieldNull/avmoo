A spider for porn sites: [www.avmoo.net](http://www.avmoo.net), [www.avless.net](http://www.avless.net), [www.avmemo.net](http://www.avmemo.net)

### Spider

Use a pool of rotating IPs from [pachong.org](http://pachong.org/transparent.html) which is stored in [proxies.txt](proxies.txt).

Rotate the http user agent from a pool which is stored in [agents.txt](agents.txt)


### Usage

``` python
db = DBManager(user='root', passwd='passwd', db='avmoo')
db.create_db() # don't create twice

spider = Spider(domain='www.avmoo.net') # domain

# You can run those functions in the order
down_star(spider, db, 182) # 182 is the total page of actresses
#down_movie_id(spider, db)
#down_movie_detail(spider, db)
```

You'd better `test_proxies()` at first.


### Mirror Site

Using the acquired data, I build a mirror site: [www.fuckthedog.cc](http://www.fuckthedog.cc)

Database can be downloaded from [here](https://www.dropbox.com/s/gy2u6a0qzeh4bcu/avmoo.sql?dl=0) and below is the schema:

( For simplicity, every field is `VARCHAR(127)` )

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

