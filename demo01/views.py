""" Cornice services.
"""
from cornice import Service
from pyramid.view import view_config, notfound_view_config
from pyramid.httpexceptions import \
		HTTPNotFound, HTTPException, HTTPBadRequest, HTTPServiceUnavailable
from pyramid.response import FileResponse, FileIter
from demo01.validators import validToken, validAdminToken1, validAdminToken2
from demo01.csv2Json.csv2Json import Csv2Json
from demo01.json2Csv.json2Csv import Json2Csv
import os

import logging
log = logging.getLogger(__name__)

@notfound_view_config()
def notfound(request):
	request.response.status_int = 404
	return {'status': 'error', 'reason': 'Not Found'}

@view_config(context=HTTPException)
def exception_view(context, request):
	
	# set the response status code
	request.response.status_int = context.code
	return context.detail

users = Service(name='users', 
									path='/api/v1/users', 
									description="All users view")

@users.get(validators=validAdminToken1)
def get_users(request):
	"""Returns a list of all users."""
	return {'users': request.users}

# admin only service - user CRUD
user = Service(name='user', 
								path='/api/v1/user/{userName}', 
								description="User registration")

@user.get(validators=validAdminToken2)
def get_user(request):
	"""Get the user detail."""
	
	return request.validated['user']

@user.delete(validators=validAdminToken2)
def delete_user(request):
	"""Removes the user."""
	user = request.validated['user']
	request.deleteUser(user)
	user['status'] = 'success'
	return user

@user.patch(validators=validAdminToken2)
def update_user(request):
	"""Update a user."""
	user = request.validated['user']
	request.updateUser(user)
	user['status'] = 'success'
	return user
	
@user.put(validators=validAdminToken2)
def create_user(request):
	"""Add a new user."""
	user = request.validated['user']
	request.addUser(user)
	user['status'] = 'success'
	return user

# apiInfo for client users
apiInfoUser = Service(name='apiInfoUser', 
												path='/api/v1/list/{userName}', 
												description="Api service listing")

@apiInfoUser.get(validators=validToken)
def get_apiinfo_user(request):
	"""Add a new user."""

	basePath = os.path.dirname(__file__)

	return FileResponse('%s/static/userApiList.json' % basePath)

# apiInfo for the admin user
apiInfoAdmin = Service(name='apiInfoAdmin', 
												path='/api/v1/list/{userName}', 
												description="Api service listing")

@apiInfoAdmin.get(validators=validAdminToken1)
def get_apiinfo_admin(request):
	"""Add a new user."""

	basePath = os.path.dirname(__file__)

	return FileResponse('%s/static/adminApiList.json' % basePath)

# csv2json
csv2json = Service(name='csv2json', 
										path='/api/v1/csv2json/{userName}', 
										description="Transform format1 to format2")

@csv2json.post(validators=validToken)
def run_csv2json(request):

	log.debug('service : csv2json')
	
	try:
		jsonWriter = Csv2Json(request)
		jsonWriter.parse(request)
		jsonFilePath = jsonWriter.convert(request)
	except Exception as exc:
		raise HTTPBadRequest({'status':'error','reason':'%s' % str(exc)})

	return FileResponse(jsonFilePath)

# csv2jsonTest
csv2jsonTest = Service(name='csv2jsonTest', 
											path='/api/v1/csv2json/test/{userName}', 
											description="Test transform format1 to format2")

@csv2jsonTest.post(validators=validToken)
def test_csv2json(request):

	log.debug('service : csv2jsonTest')

	try:
		jsonWriter = Csv2Json(request)
		jsonWriter.parse(request)
	except Exception as exc:
		raise HTTPBadRequest({'status':'error','reason':'%s' % str(exc)})

	return {'status':'success'}

# json2csv
json2csv = Service(name='json2csv', 
										path='/api/v1/json2csv/{userName}', 
										description="Transform format1 to format2")

@json2csv.post(validators=validToken)
def run_json2csv(request):

	log.debug('service : json2csv')
	response = request.response
	
	try:
		csvWriter = Json2Csv(request)
		csvWriter.parse(request)
		csvWriter.convert(request)
	except Exception as exc:
		try:
			log.error('@run_json2csv:1, ' + str(exc))
			# zipfile will contain only a json error report txt file
			response.status_int = 400
			zfh = csvWriter.get400ErrZipFile(request, str(exc))
		except Exception as exc:
			log.error('@run_json2csv:2\n')
			raise500Error(request, str(exc))
	else:
		try:
			zfh = csvWriter.getCsvZipFile()
		except Exception as exc:
			raise500Error(request, str(exc))
		
	# rewind zipfile back to start of the file
	zfh.seek(0)

	# let the factory response set the content_length
	# because a length compare mismatch will cause an exception
	response.content_type = 'application/zip'
	response.app_iter = FileIter(zfh)
	return response

# json2csvTest
json2csvTest = Service(name='json2csvTest', 
											path='/api/v1/json2csv/test/{userName}', 
											description="Test transform format1 to format2")

@json2csvTest.post(validators=validToken)
def test_json2csv(request):

	log.debug('service : json2csvTest')

	try:
		csvWriter = Json2Csv(request)
		csvWriter.parse(request)
	except Exception as exc:
		raise HTTPBadRequest({'status':'error','reason':str(exc)})

	if csvWriter.hasJsonDecodeErrors(request=request):
		return csvWriter.get400ErrJsonFile()

	return {'status':'success'}

#def postLogMsg(place, errmsg, reason):
	
def raise500Error(request, reason):

	outputDir = request.getWorkSpace('outputCsv')
		
	zipFilePath = '%s/json2csv.zip' % outputDir
		
	errmsg = 'failed to create zipfile : %s\n reason : %s' 
	errmsg = errmsg %	(zipFilePath, reason)
	log.error(errmsg)
	raise HTTPServiceUnavailable({'status':'error', 
																'reason': 'failed to create zipfile'})

	
