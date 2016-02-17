# coding:utf-8

"""
A Flask APP
"""

import datetime

import re
from flask import Flask, render_template, request, redirect, url_for, abort
from peewee import CharField, ForeignKeyField, CompositeKey, fn
from playhouse.flask_utils import FlaskDB, object_list, get_object_or_404

DATABASE = {
    'name': 'avmoo',
    'user': 'root',
    'passwd': '19961020',
    'host': '127.0.0.1',
    'port': 3306,
    'engine': 'playhouse.pool.PooledMySQLDatabase',
    'max_connections': 32,
    'stale_timeout': 10
}

app = Flask(__name__)
app.config.from_object(__name__)
app.debug = True

database = FlaskDB(app)
paginate_by = 36


@app.route('/')
def index():
    query = Movie.select().order_by(Movie.time.desc())
    return object_list('index.html', query=query, context_variable='movies', paginate_by=paginate_by)


@app.route('/released')
def released():
    now = datetime.datetime.now()
    time = now.strftime("%Y-%m-%d %H:%M")
    query = Movie.select().where(Movie.time <= time).order_by(Movie.time.desc())
    return object_list('index.html', query=query, context_variable='movies', paginate_by=paginate_by)


@app.route('/movie/<mid>')
def movie(mid):
    movie = get_object_or_404(Movie, Movie.mid == mid)
    actors = Star.select().join(MovieActor).where(MovieActor.mid == mid)
    samples = MovieSample.select().join(Movie).where(Movie.mid == mid)
    cates = MovieCate.select().join(Movie).where(Movie.mid == mid)
    return render_template('movie.html', movie=movie, actors=actors, samples=samples, cates=cates)


@app.route('/star')
def stars():
    query = Star.select(Star, fn.COUNT(MovieActor.mid)) \
        .join(MovieActor).group_by(Star.sid).order_by(fn.COUNT(MovieActor.mid).desc())
    return object_list('stars.html', query=query, context_variable='stars', paginate_by=paginate_by)


@app.route('/tag')
def tags():
    tags = MovieCate.select(MovieCate.cate, fn.COUNT(MovieCate.mid)) \
        .group_by(MovieCate.cate).distinct().order_by(MovieCate.cate.desc())
    return render_template('tags.html', tags=tags)


@app.route('/star/<sid>')
def star_movie(sid):
    query = Movie.select().join(MovieActor).join(Star).where(Star.sid == sid).order_by(Movie.time.desc())
    return object_list('index.html', query=query, context_variable='movies', paginate_by=paginate_by)


@app.route('/tag/<tag>')
def tag_movie(tag):
    query = Movie.select().join(MovieCate).where(MovieCate.cate == tag).order_by(Movie.time.desc())
    return object_list('index.html', query=query, context_variable='movies', paginate_by=paginate_by)


@app.route('/search')
def search():
    word = request.args.get('q').strip()
    match = re.search('([a-zA-Z]+)\s*-?\s*(\d+)', word)
    if not match:
        return abort(404)

    word = ('%s-%s' % (match.group(1), match.group(2))).upper()
    movie = get_object_or_404(Movie, Movie.id == word)
    return redirect(url_for('movie', mid=movie.mid))


class Movie(database.Model):
    mid = CharField(primary_key=True)
    id = CharField()
    name = CharField()
    time = CharField()
    length = CharField()
    cover = CharField()
    small = CharField()


class Star(database.Model):
    sid = CharField(primary_key=True)
    name = CharField()
    img = CharField()


class MovieActor(database.Model):
    mid = ForeignKeyField(Movie)
    sid = ForeignKeyField(Star)

    class Meta:
        primary_key = CompositeKey('mid', 'sid')
        db_table = 'movie_actor'


class MovieSample(database.Model):
    mid = ForeignKeyField(Movie)
    img = CharField()

    class Meta:
        primary_key = CompositeKey('mid', 'img')
        db_table = 'movie_sample'


class MovieCate(database.Model):
    mid = ForeignKeyField(Movie)
    cate = CharField()

    class Meta:
        primary_key = CompositeKey('mid', 'cate')
        db_table = 'movie_cate'


if __name__ == '__main__':
    app.run()
