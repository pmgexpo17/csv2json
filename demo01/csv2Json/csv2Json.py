#----------------------------------------------------------------------#
# Csv2Json 
#
# - converts csv file data to python objects, storing the object parts
# in a graph datastore. Finally retrieves the parts and compiles the 
# complete json object then converts python to json, writing the text to
# and output json file
#
# pmg - 20/06/2017: beta version
#----------------------------------------------------------------------# 
from collections import OrderedDict
import csv
import os
import shelve
import simplejson as json
import zipfile

import logging
log = logging.getLogger(__name__)

class Csv2Json(object):

	#------------------------------------------------------------------#
	# constructor parms -
	# - request: contains the settings hash
	#------------------------------------------------------------------#
	def __init__(self, request):
		self.sizeLimit = 500
		
		if self.hasAppParameter(request, 'sizeLimit'):
			self.sizeLimit = int(request.registry.settings['sizeLimit'])

	# test if app param exists in app settings
	def hasAppParameter(self, request, paramKey):

		try:
			request.registry.settings[paramKey]
		except KeyError:
			errmsg = '@hasAppParameter, %s is not available in app settings'
			log.error(errmsg % paramKey)
			return False
			
		return True

	# In this next section are methods for adding the csv records to the
	# graph cache 
	# Note : since a dictionary randomizes the key order, must store the
	# object as a list of tuples and then convert it to an OrderedDict
	# when writing the csv
	
	# For each json policy child objects, open the related csv, iterate,
	# adding each python object to the graph store 
	def putCsvDataAll(self):
		
		try:
			self.sizeLimitError = False
			self.objProvider = self.getDbObjProvider()
			self.db['policyId'] = []
			for objName in self.objProvider.keys():
				self.putCsvData(objName)
				if objName == 'PolicyDetails' and self.hitSizeLimit:
					self.sizeLimitError = True
		except Exception as exc:
			log.error(str(exc))
			raise
			
	# Open the csv, iterate passing the key and value list objects to
	# the graph object handler 
	def putCsvData(self,objName):

		self.hitSizeLimit = False
		self.csvKeys = {}
		csvFilename = '%s/%s.csv' % (self.csvExtractDir,objName)
		if not os.path.exists(csvFilename):
			errmsg = 'csv srcfile %s not found in zip archive'
			log.error('@putCsvData, ' + errmsg % objName)
			raise Exception(errmsg % objName)
		try:
			with open(csvFilename) as fh:
				csvReader = csv.reader(fh,quotechar='"', 
																		doublequote=False, escapechar='\\')
				keys = next(csvReader)
				self.recnum = 1
				for values in csvReader:					
					if self.recnum > self.sizeLimit:
						self.hitSizeLimit = True
						log.info('hit size limit[%d] for %s' % (self.recnum, objName))
						break
					self.objProvider[objName](keys, values)
		except csv.Error as exc:
			errmsg = 'csv reader error, file: %s, line: %d, %s' \
															% (objName, csvReader.line_num, str(exc))
			log.error('@putCsvData, %s' % errmsg)
			raise Exception(errmsg)
			
	# Build a policy object and store it
	def putPolicyObj(self, keys, values):

		try:
			record = dict(zip(keys,values))
			pkvalue = record['PolicyRiskSubitemID']
			self.db[pkvalue] = list(zip(keys,values))
			self.appendValue('policyId', pkvalue)
			self.recnum += 1
		except KeyError as exc:
			log.error('@putPolicyObj, ' + str(exc))
			raise

	# This is to avoid creating the keyStore with writeBack=True which
	# consumes memory and could be a deal breaker if the input is large
	def appendValue(self, dbObjKey, value):
		
		self.db[dbObjKey] += [value]
		
	# Build a driver list and store it
	def putDriverObj(self, keys, values):

		try:
			record = dict(zip(keys,values))
			pkvalue = record['RiskDriverSubitemID']
			self.db[pkvalue] = record
			fkvalue = record['PolicyRiskSubitemID']
			driverObjKey = '%s:driver' % fkvalue
			record = list(zip(keys,values))
			if driverObjKey in self.db:
				self.appendValue(driverObjKey, record)
			else:
				self.db[driverObjKey] = [record]
				self.recnum += 1
		except KeyError as exc:
			log.error('@putDriverObj, ' + str(exc))
			raise
		
	# Build a driver claims list and store it
	def putDriverClaimsObj(self, keys, values):

		try:
			record = dict(zip(keys,values))
			fkvalue = record['RiskDriverSubitemID']
			claimsObjKey = '%s:claims' % fkvalue
			record = list(zip(keys,values))
			if claimsObjKey in self.db:
				self.appendValue(claimsObjKey, record)
			else:
				self.db[claimsObjKey] = [record]
				self.recnum += 1
		except KeyError as exc:
			log.error('@putDriverClaimsObj, ' + str(exc))
			raise
		
	# Build a premium variation list and store it
	def putPremVariationObj(self, keys, values):
		
		try:
			record = dict(zip(keys,values))
			fkvalue = record['PolicyRiskSubitemID']
			premVarObjKey = '%s:premiumVar' % fkvalue
			record = list(zip(keys,values))
			if premVarObjKey in self.db:
				self.appendValue(premVarObjKey, record)
			else:
				self.db[premVarObjKey] = [record]
				self.recnum += 1
		except KeyError as exc:
			log.error('@putPremVariationObj, ' + str(exc))
			raise

	
	# Build a strategic object and store it
	def putStrategicObj(self, keys, values):
		
		try:
			record = dict(zip(keys,values))
			fkvalue = record['PolicyRiskSubitemID']
			strategicObjKey = '%s:strategic' % fkvalue
			record = list(zip(keys,values))
			if strategicObjKey in self.db:
				self.appendValue(strategicObjKey, record)
			else:
				self.db[strategicObjKey] = [record]
				self.recnum += 1
		except KeyError as exc:
			log.error('@putStrategicObj, ' + str(exc))
			raise

	# Install each handler in a map for lookup by child object name
	def getDbObjProvider(self):
		
		return {'PolicyDetails': self.putPolicyObj,
				'DriverDetails': self.putDriverObj,
				'DriverClaimsDetails': self.putDriverClaimsObj,
				'PremiumVariationDetails': self.putPremVariationObj,
				'StrategicDetails': self.putStrategicObj}
				
	# In this section are methods to retrieve, compile and write json 
	# objects Note : the compiled object is converted to an OrderedDict
	# This maintains the input csv field order
	
	# The input policyId index is cached for easy iteration. For each
	# policyId, retrieve each child object by graph key lookup, then
	# compile and write the json object to the output file. Provide a
	# sample size when running pretty print 
	def writeJsonObjAll(self):

		with open(self.jsonOutputFile,'w') as fh:
			if self.sizeLimitError:
				self.writeSizeLimitError(fh)
			for policyId in self.db['policyId']:
				self.writeJsonObj(policyId, fh)

	# put a size limit error json message
	def writeSizeLimitError(self, fh):
		
		reason = 'input csv record volume limit[%d] exceeded' % self.sizeLimit
		errmsg = '{"status":"warn","reason":"%s"}\n'
		fh.write(errmsg % reason)
		
	# Convert the python object to json text and write to the output file
	def writeJsonObj(self, policyId, fh):

		policyObj = self.getPolicyObj(policyId)
		fh.write('%s\n' % json.dumps(policyObj))
		
	# Return a policy object, adding all the child components to the
	# policy details object
	def getPolicyObj(self, policyId):

		policyObj = OrderedDict(self.db[policyId])
		driverListKey = '%s:driver' % policyId
		policyObj['DriverDetails'] = self.getDriverObj(driverListKey)
		premVarListKey = '%s:premiumVar' % policyId
		policyObj['PremiumVariationDetails'] = \
																		self.getPremVarObj(premVarListKey)
										
		strategyListKey = '%s:strategy' % policyId
		policyObj['StrategicDetails'] = self.getStrgyObj(strategyListKey)
		return {'PolicyDetails':policyObj}
		
	# Return a driver object, adding the claims child object
	def getDriverObj(self,listKey):
		
		if listKey not in self.db:
			return []
		driverList = []
		for driverObj in self.db[listKey]:
			driverObj = OrderedDict(driverObj)
			driverId = driverObj['RiskDriverSubitemID']
			claimsObjKey = '%s:%s' % (driverId, 'claims')
			if claimsObjKey in self.db:
				driverClaimsObj = list(OrderedDict(claimsObj) 
																for claimsObj in self.db[claimsObjKey])
				driverObj['DriverClaimsDetails'] = driverClaimsObj
			else:
				driverObj['DriverClaimsDetails'] = self.getEmptyClaimsObj()			
			driverList.append(driverObj)
		return driverList

	# Return an empty driver claims object, an Ernix requirement
	def getEmptyClaimsObj(self):
	
		return [OrderedDict([("DriverClaimType", ""),
										 		 ("ClaimYear", ""), 
										 		 ("ClaimMonth", ""),
										 		 ("RiskDriverSubitemID", "")])]

	# Return a premium variation object
	def getPremVarObj(self,listKey):
		
		if listKey not in self.db:
			return [{}]
		return list(OrderedDict(premVarObj) for premVarObj in self.db[listKey])

	# Return a strategic object
	def getStrgyObj(self,listKey):
		
		if listKey not in self.db:
			return [{}]
		return list(OrderedDict(strategicObj) for strategicObj in self.db[listKey])

	# -------------------------------------------------------------- #
	# parse
	# ---------------------------------------------------------------#
	def parse(self, request):

		try:		
			inputZipFile = request.POST['srcfile'].file
		except KeyError as exc:
			log.error('@parse, file upload param srcfile is missing')
			raise Exception('file upload param srcfile is missing')
		
		csvExtractDir = request.getWorkSpace('inputCsv')
			
		try:
			_zipFile = zipfile.ZipFile(inputZipFile, "r")
		except zipfile.BadZipfile as exc:
			log.error('@parse, bad zip file')
			raise Exception('bad zip file: ' + str(exc))
		else:
			_zipFile.extractall(csvExtractDir)
			_zipFile.close()
			
		self.csvExtractDir = csvExtractDir

		dbFilePath = '%s/pyObjStore.bdat' % self.csvExtractDir
		try:
			self.db = shelve.open(dbFilePath, flag='n')
		except IOError as exc:
			log.error('@parse, ' + os.strerror(exc.errno))
			raise Exception(os.strerror(exc.errno))

		log.info('about to parse csv and compile json data ..')
		self.putCsvDataAll()

	# -------------------------------------------------------------- #
	# Main method
	# ---------------------------------------------------------------#
	def convert(self, request):

		outputDir = request.getWorkSpace('outputJson')
		
		self.jsonOutputFile = '%s/csv2json.txt' % outputDir

		log.info('about to write json output file ..')
		self.writeJsonObjAll()
		log.info('done. Number of exported policies : %s'
																						% len(self.db['policyId']))
		return self.jsonOutputFile
