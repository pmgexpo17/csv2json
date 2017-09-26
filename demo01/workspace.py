import os
import binascii
from pyramid.events import NewRequest

import logging
log = logging.getLogger(__name__)

def includeme(config):

	# make a temporary workspace
	def _make_work_space(request, appMode=None):
		
		workSpace = makeTempSpace(request, appMode)
		if not workSpace:
			workSpace = makeTempSpace(request, appMode)
			if not workSpace:
				raise Exception('failed to make temp workspace')
			
		return workSpace

	def makeTempSpace(request, appMode):

		try:
			if not hasattr(request,'workSpace'):
				#settings = request.registry.settings
				basePath = os.path.dirname(__file__)
				tempSpace = binascii.b2a_hex(os.urandom(20))
				#workSpace = '%s/work/%s' % (settings['tempBase'], tempSpace)
				workSpace = '%s/work/%s' % (basePath, tempSpace)
				if os.path.exists(workSpace):
					log.warn('exclusive workspace already exists : %s' % workSpace)
					return None
				else:
					os.makedirs(workSpace)
					request.workSpace = workSpace

			if not appMode:
				return request.workSpace
				
			appSpace = '%s/%s' % (request.workSpace, appMode)
			if not os.path.exists(appSpace):
				os.mkdir(appSpace)
					
			return appSpace
		except Exception as exc:
			exc.message = 'failed to make temp workspace'
			raise
	
	# remove a temporary workspace
	def _remove_work_space(request):

		try:
			if os.path.exists(request.workSpace):
				os.system('rm -rf %s/' % request.workSpace)
		except AttributeError:
			pass # validation error has aborted request before workspace is created
		except Exception as exc:
			log.error('failed to remove workspace : %s' % request.workSpace)
		
	def setup_post_request(event):
		event.request.add_finished_callback(_remove_work_space)

	config.add_request_method(_make_work_space,'getWorkSpace')
	config.add_subscriber(setup_post_request, NewRequest)
