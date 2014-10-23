#!/usr/bin/env python
# coding: utf-8

import click

@click.group()
def cli():
    """Simple web spider/server"""

@cli.command()
@click.argument('url')
@click.argument('database')
def spider(url, database):
    from  fetcher import Fetcher
    with Fetcher(url, database) as fetcher:
        fetcher.run()

@cli.command()
@click.argument('database')
def serve(database):
    from serve import Server
    from werkzeug.serving import run_simple
    app = Server(database)
    run_simple('localhost', 8080, app, use_reloader=True, threaded=True)

@cli.command()
@click.argument('database')
def dump(database):
    import shelve
    db = shelve.open(database, 'r')

    from pprint import pprint
    pprint(dict(db.items()))

if __name__ == '__main__':
    cli()
