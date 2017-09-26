"""Main entry point
"""
from pyramid.config import Configurator
from pyramid.renderers import json_renderer_factory, JSON

def main(global_config, **settings):
	config = Configurator(settings=settings)
	config.add_static_view('static', 'static', cache_max_age=3600)
	config.add_renderer(None, json_renderer_factory)
	config.add_renderer('prettyjson', JSON(indent=4))    
	config.include("cornice")
	config.include("demo01.workspace")
	config.include("demo01.usersDb")
	config.scan("demo01.views")
	return config.make_wsgi_app()

