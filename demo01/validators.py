import os
import binascii

from pyramid.httpexceptions import \
												HTTPUnauthorized, HTTPBadRequest, HTTPNotFound

# _create_token
def _create_token():
	return binascii.b2a_hex(os.urandom(20))

# validToken
def validToken(request, **kwargs):
	header = 'X-Api-Token'
	xtoken = request.headers.get(header)
	if xtoken is None:
		raise HTTPUnauthorized({'status': 'error', 'reason': "Unauthorized"})

	userName = str(request.matchdict['userName'])
	valid = userName in request.users and request.getUserToken(userName) == xtoken
	if not valid:
		raise HTTPUnauthorized({'status': 'error', 'reason': "Unauthorized"})

# validAdminToken
def validAdminToken1(request, **kwargs):
	header = 'X-Api-Token'
	xtoken = request.headers.get(header)
	if xtoken is None:
		raise HTTPUnauthorized({'status': 'error', 'reason': "Unauthorized"})

	valid = request.getUserToken('admin') == xtoken
	
	if not valid:
		raise HTTPUnauthorized({'status': 'error', 'reason': "Unauthorized"})

def validAdminToken2(request, **kwargs):
	header = 'X-Api-Token'
	xtoken = request.headers.get(header)
	if xtoken is None:
		raise HTTPUnauthorized({'status': 'error', 'reason': "Unauthorized"})

	valid = request.getUserToken('admin') == xtoken
	
	if not valid:
		raise HTTPUnauthorized({'status': 'error', 'reason': "Unauthorized"})

	userName = str(request.matchdict['userName'])
	
	if request.method == 'GET':
		if userName in request.users:
			user = {'name': userName, 'token': request.getUserToken(userName)}
			request.validated['user'] = user
			return
		else:
			raise HTTPBadRequest({'status': 'error', 'reason': "User doesn't exist"})
	
	if request.method == 'DELETE':
		if userName in request.users:
			request.validated['user'] = {'name': userName}
			return
		else:
			raise HTTPBadRequest({'status': 'error', 'reason': "User doesn't exist"})

	if request.method == 'PATCH':
		if userName in request.users or userName == 'admin':
			user = {'name': userName, 'token': _create_token()}
			request.validated['user'] = user
		else:
			raise HTTPBadRequest({'status': 'error', 'reason': "User doesn't exist"})

	if request.method == 'PUT':
		if userName in request.users:
			raise HTTPBadRequest({'status': 'error', 'reason': 'User aleady exists'})
		else:
			user = {'name': userName, 'token': _create_token()}
			request.validated['user'] = user
