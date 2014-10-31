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
    from fetcher import Fetcher
    Fetcher(url, database).run()

@cli.command()
@click.argument('database')
def serve(database):
    from serve import Server
    from werkzeug.serving import run_simple

    app = Server(database)
    try:
        run_simple('localhost', 8080, app, threaded=True)
    except KeyboardInterrupt:
        print "KILLING?"
        raise

@cli.command()
@click.argument('database')
def extract(database):
    from extractor import Extractor
    Extractor(database).run()

@cli.command()
@click.argument('database')
def fix(database):
    from fixer import Fixer
    Fixer(database).run()

@cli.command()
@click.argument('database')
def dump(database):
    import os
    import json
    for root, dirs, files in os.walk(database):
        for filename in files:
            if not filename.endswith('.data'):
                continue
            with open(os.path.join(root, filename[:-5])) as fh:
                data = json.load(fh)
                print data['url']

if __name__ == '__main__':
    cli()
