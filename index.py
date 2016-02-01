# coding:utf-8
from flask import Flask, render_template

app = Flask(__name__)
app.config.from_object(__name__)
app.debug = True


@app.route('/')
def index():
    return render_template('index.html')


app.run()
