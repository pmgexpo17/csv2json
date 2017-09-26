#----------------------------------------------------------------------#
# Json2Csv 
#
# - converts json objects to python objects, storing the object parts
# in a graph datastore. Finally retrieves the parts converting the python
# object to 1 or more csv records, writing the text to a corresponding
# output csv file
#
# pmg - 20/06/2017: beta version
#----------------------------------------------------------------------# 
from pyramid.response import FileResponse
from collections import OrderedDict
import os
import shelve
import simplejson as json
import zipfile

import logging
log = logging.getLogger(__name__)

class Json2Csv(object):
	
	#------------------------------------------------------------------#
	# constructor parms -
	# - dlm: delimiter for record value separation
	#------------------------------------------------------------------#
	def __init__(self, request):
		self.dlm = ','
		self.sizeLimit = 500
		
		if self.hasAppParameter(request, 'sizeLimit'):
			self.sizeLimit = int(request.registry.settings['sizeLimit'])
			
		if self.hasAppParameter(request, 'recordDelimiter'):
			self.dlm = request.registry.settings['recordDelimiter']
			
	# test if app param exists in app settings
	def hasAppParameter(self, request, paramKey):

		try:
			request.registry.settings[paramKey]
		except KeyError:
			errmsg = '@hasAppParameter, %s is not available in app settings'
			log.error(errmsg % paramKey)
			return False
			
		return True
		
	# For each json child object, write the output csv. The dbKeyTag
	# is the datastore key suffix for object retrieval
	def writeCsvFileAll(self):
		
		if self.db['policyId'] == []:
			return
		self.csvTextPrvdr = self.getCsvTextProvider()
		metaDbObj = {'PolicyDetails': 'policy',
									'DriverDetails': 'driver',
									'DriverClaimsDetails': 'claims',
									'PremiumVariationDetails': 'premiumVar',
									'StrategicDetails': 'strategic'}
		for jsonObjName, dbKeyTag in metaDbObj.items():
			self.writeCsvFile(jsonObjName,dbKeyTag)

	# Create the output csv, iterate by policyId calling the csv export
	# handler to write the records. The input policyId index in cached 
	# for easy iteration. For each policyId, retrieve each child object 
	# by graph key lookup
	def writeCsvFile(self, jsonObjName, dbKeyTag):

		try:
			csvFilename = '%s/%s.csv' % (self.outputDir,jsonObjName)
			with open(csvFilename,'w') as self.fh:
				self.writeHeader(jsonObjName, dbKeyTag)
				for pkvalue in self.db['policyId']:
					recordKey = '%s:%s' % (pkvalue, dbKeyTag)
					if recordKey in self.db:
						self.csvTextPrvdr[jsonObjName](recordKey)
						
		except IOError as exc:
			log.error('@59, ' + os.strerror(exc.errno))
			raise Exception(os.strerror(exc.errno))

	def writeHeader(self, jsonObjName, dbKeyTag):

		dbHeaderKey = 'header:%s' % dbKeyTag		
		csvRecord = self.dlm.join(self.db[dbHeaderKey]) 
		self.fh.write(csvRecord + '\n')

	# Retrieve the list of graph pointers, iterate, calling the list 
	# object csv export handler
	def putCsvListRefText(self, recordKey):			
		objRefList = self.db[recordKey]
		for objListKey in objRefList:
			self.putCsvListText(objListKey)
			
	# If the python list is not empty, make a sequence of csv records
	# and export them
	def putCsvListText(self, recordKey):
		recordList = self.db[recordKey]
		if not recordList:
			return
		csvRecList = []
		for record in recordList:
			csvRecord  = self.dlm.join(str(x) for x in record)
			csvRecList.append(csvRecord)
		self.fh.write('\n'.join(csvRecList) + '\n')

	# Make a single csv record and export it
	def putCsvObjText(self, recordKey):
		record = self.db[recordKey]
		csvRecord = self.dlm.join(str(x) for x in record)
		self.fh.write(csvRecord + '\n')
		
	# Install each handler in a map for lookup by child object name
	def getCsvTextProvider(self):
		
		return {'PolicyDetails': self.putCsvObjText,
				'DriverDetails': self.putCsvListText,
				'DriverClaimsDetails': self.putCsvListRefText,
				'PremiumVariationDetails': self.putCsvListText,
				'StrategicDetails': self.putCsvListText}

	# open the json input, iterate, calling the object storage handler
	def putPolicyObjAll(self):

		self.db['policyId'] = []
		self.db['jsonDecodeError'] = []
		self.sizeLimitError = False
		recnum = 1
		self.headerIsDone = False
		for jsonRecord in self.jsfh:
			try:
				if not self.headerIsDone:
					self.putPolicyHeader(jsonRecord)
				self.putPolicyObj(jsonRecord)
				if recnum > self.sizeLimit:
					self.sizeLimitError = True
					break
			except ValueError as exc:
				self.appendValue('jsonDecodeError',recnum)
				dbObjKey = 'jsonDecodeError:%d' % recnum
				self.db[dbObjKey] = str(exc)
			finally:
				recnum += 1
	
	def putPolicyHeader(self, jsonRecord):
		
		try:
			pdRecord = json.loads(jsonRecord, object_pairs_hook=OrderedDict)
			pdRecord = pdRecord.pop('PolicyDetails')
			driverRecord = pdRecord.pop('DriverDetails')[0]
			driverClaimsRecord = driverRecord.pop('DriverClaimsDetails')[0]
			premVarRecord = pdRecord.pop('PremiumVariationDetails')[0]
			strategicRecord = pdRecord.pop('StrategicDetails')[0]
			self.db['header:driver'] = list(driverRecord.keys())
			self.db['header:claims'] = list(driverClaimsRecord.keys())
			self.db['header:premiumVar'] = list(premVarRecord.keys())
			strategicKeys = list(strategicRecord.keys())
			if not strategicKeys:
				strategicKeys = ['PolicyRiskSubitemID']
			self.db['header:strategic'] = strategicKeys
			self.db['header:policy'] = list(pdRecord.keys())
			self.headerIsDone = True
			# put the policy details as well
		except KeyError as exc:
			log.error('@putPolicyHeader, missing key : ' + str(exc))
			# recorded a decode error so the user can correct the data
			raise ValueError('missing key : ' + str(exc))
		
		
	# Extract each policy child component, make the graph key and store
	# the python objects
	def putPolicyObj(self, jsonRecord):

		try:
			pdRecord = json.loads(jsonRecord, object_pairs_hook=OrderedDict)
			pdRecord = pdRecord.pop('PolicyDetails')
			pkvalue = pdRecord['PolicyRiskSubitemID']
			#log.debug('PolicyRiskSubitemID : ' + pkvalue)		
			driverList = pdRecord.pop('DriverDetails')
			self.putDriverObj(pkvalue, driverList)
			premVarObjKey = '%s:premiumVar' % pkvalue
			premVarList = pdRecord.pop('PremiumVariationDetails')			
			self.db[premVarObjKey] = list(list(premVarObj.values()) for
											premVarObj in premVarList)
			strgcObjKey = '%s:strategic' % pkvalue
			strgcList = pdRecord.pop('StrategicDetails')
			if strgcList[0]:
				self.db[strgcObjKey] = list(list(strgcObj.values()) for 
												strgcObj in strgcList)
			else:
				self.db[strgcObjKey] = [[pkvalue]]
			policyObjKey = '%s:policy' % pkvalue
			self.db[policyObjKey] = list(pdRecord.values())
			self.appendValue('policyId', pkvalue)
		except KeyError as exc:
			log.error('@putPolicyObj, missing key : ' + str(exc))
			raise ValueError('missing key : ' + str(exc))
			
	# This is to avoid creating the keyStore with writeBack=True which
	# consumes memory and could be a deal breaker if the input is large
	def appendValue(self, dbObjKey, value):
		
		self.db[dbObjKey] += [value]

	# Extract the claims child object, make the graph key and store
	# the python objects
	def putDriverObj(self, fkvalue, driverDtlList):

		try:
			claimsObjRefList = []
			driverObjList = []
			for driverRecord in driverDtlList:
				driverClaimsList = driverRecord.pop('DriverClaimsDetails')
				driverObjList.append(list(driverRecord.values()))
				# only add non-empty claim records 
				if not driverClaimsList[0]['ClaimYear']:
					continue
				pkvalue = driverRecord['RiskDriverSubitemID']
				claimsObjRefList.append(pkvalue)
				self.db[pkvalue] = list(list(claimsObj.values()) for
										claimsObj in driverClaimsList)
			claimsObjKey = '%s:claims' % fkvalue
			self.db[claimsObjKey] = claimsObjRefList
			driverObjKey = '%s:driver' % fkvalue
			self.db[driverObjKey] = driverObjList
		except KeyError as exc:
			log.error('@putDriverObj, missing key : ' + str(exc))
			raise ValueError('missing key : ' + str(exc))

	# -------------------------------------------------------------- #
	# hasJsonDecodeErrors
	# ---------------------------------------------------------------#		
	def hasJsonDecodeErrors(self, request=None):

		jsonDecodeError = len(self.db['jsonDecodeError']) > 0

		if not jsonDecodeError and not self.sizeLimitError:
			return False

		# 2 cases : 
		# run mode : self.outputDir is made, request=None		
		# test mode : self.outputDir is not made, request=request
		if request:
			self.outputDir = request.getWorkSpace('outputCsv')
		
		self.jsonErrLogPath = '%s/jsonDecodeErrors.txt' % self.outputDir
		
		reason1 = 'input json record volume limit[%d] exceeded' % self.sizeLimit
		errmsg1 = '{"status":"%s","reason":"%s"}\n'
		errmsg2 = '{"status":"error","recnum":%d,"reason":"%s"}\n'
		with open(self.jsonErrLogPath,'w') as fh:
			if self.sizeLimitError:
				errRecord = errmsg1 % ('warn',reason1)
				fh.write(errRecord)
			if jsonDecodeError:
				errRecord = errmsg1 % ('error','json decode errors')
				fh.write(errRecord)
				for recnum in self.db['jsonDecodeError']:
					dbObjKey = 'jsonDecodeError:%d' % recnum
					errRecord = errmsg2 % (recnum, self.db[dbObjKey])
					fh.write(errRecord)

		return True

	# -------------------------------------------------------------- #
	# get400ErrJsonFile
	# ---------------------------------------------------------------#
	def get400ErrJsonFile(self):

		response = FileResponse(self.jsonErrLogPath)
		response.status_int = 400
		return response

	# -------------------------------------------------------------- #
	# get400ErrZipFile
	# ---------------------------------------------------------------#
	def get400ErrZipFile(self, request, reason):

		outputDir = request.getWorkSpace('outputCsv')
		
		zipFilePath = '%s/json2csv.zip' % outputDir
	
		zfh = open(zipFilePath,'w+b')	
	
		# create error zipfile and error message
		with zipfile.ZipFile(zfh, 'w') as zobj:
			zobj.writestr('error.json','{"status":"error":"reason":"%s"}\n' % reason)

		return zfh

	# -------------------------------------------------------------- #
	# getCsvZipFile
	# ---------------------------------------------------------------#
	def getCsvZipFile(self):

		os.chdir(self.outputDir)
		
		zipFilePath = '%s/json2csv.zip' % self.outputDir

		zfh = open(zipFilePath,'w+b')

		# create zipfile and add csv files
		with zipfile.ZipFile(zfh, 'w') as zobj:
			if self.hasJsonDecodeErrors():
				zobj.write('jsonDecodeErrors.txt')
			if self.db['policyId'] != []:
				zobj.write('PolicyDetails.csv')
				zobj.write('DriverDetails.csv')
				zobj.write('DriverClaimsDetails.csv')
				zobj.write('PremiumVariationDetails.csv')
				zobj.write('StrategicDetails.csv')
		
		return zfh
		
	# -------------------------------------------------------------- #
	# parse
	# ---------------------------------------------------------------#		
	def parse(self, request):

		try:		
			srcJsonFile = request.POST['srcfile'].file
		except KeyError as exc:
			log.error('@parse, file upload param srcfile is missing')
			raise Exception('file upload param srcfile is missing')

		self.inputDir = request.getWorkSpace('inputJson')

		dbFilePath = '%s/pyObjStore.bdat' % self.inputDir
		try:
			self.db = shelve.open(dbFilePath, flag='n')
		except IOError as exc:
			log.error('@parse, ' + os.strerror(exc.errno))
			raise Exception(os.strerror(exc.errno))
	
		jsonFilePath = '%s/json2csv.txt' % self.inputDir
	
		chunk_size = 8192
		srcJsonFile.seek(0)
		with open(jsonFilePath, 'w+') as self.jsfh:
			for chunk in iter(lambda: srcJsonFile.read(8192), b''):
				self.jsfh.write(chunk.decode('utf-8'))
			self.jsfh.seek(0)
			log.info('about to parse json and compile csv data ..')		
			self.putPolicyObjAll()

	# -------------------------------------------------------------- #
	# Main method
	# ---------------------------------------------------------------#		
	def convert(self, request):

		self.outputDir = request.getWorkSpace('outputCsv')

		log.info('about to write csv files ..')		
		self.writeCsvFileAll()
		log.info('done. Number of imported policies : %s' 
																					% len(self.db['policyId']))
		
