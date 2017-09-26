import os
import shelve

import logging
log = logging.getLogger(__name__)

def includeme(config):

	try:
		# Store DB connection in registry
		settings = config.registry.settings
		userDbSpace = '%s/userdb' % settings['tempBase']
		if not os.path.exists(userDbSpace):
			os.makedirs(userDbSpace)

		userObjStore = '%s/userObjStore.bat' % userDbSpace
		userDb = shelve.open(userObjStore,flag='c')
	except Exception as exc:
		exc.message = 'failed to create or open user database'
		raise

	try:
		userIdList = userDb['users']
	except KeyError:
		userDb['users'] = []
		
	settings['userDb'] = userDb

	def _getUserNameList(request):
		
		return request.registry.settings['userDb']['users']

	def _getUserToken(request, userName):

		try:
			userKey = 'user:%s' % userName
			userToken = request.registry.settings['userDb'][userKey]
		except KeyError:
			return None
		
		return userToken

	def _addUser(request, user):
		
		userDb = request.registry.settings['userDb']
		userDb['users'] += [user['name']]
		userKey = 'user:%s' % user['name']
		userDb[userKey] = user['token']
		userDb.sync()

	def _deleteUser(request, user):
		
		userDb = request.registry.settings['userDb']
		# since we not using shelve.writeBack must explicitly update
		users = userDb['users']
		users.remove(user['name'])
		userDb['users'] = users
		userKey = 'user:%s' % user['name']
		try :
			del(userDb[userKey])
		except:
			pass
		userDb.sync()

	def _updateUser(request, user):
		
		userDb = request.registry.settings['userDb']
		userKey = 'user:%s' % user['name']
		try :
			userDb[userKey] = user['token']
		except:
			pass
		userDb.sync()
		
	config.add_request_method(_getUserNameList,'users', property=True, reify=True)
	config.add_request_method(_getUserToken,'getUserToken')
	config.add_request_method(_addUser,'addUser')
	config.add_request_method(_deleteUser,'deleteUser')
	config.add_request_method(_updateUser,'updateUser')
