#!/usr/bin/python 
from __future__ import print_function 
import requests 
import datetime 
import time 
import sys 
from pprint import pprint 

##### Variable Declarations 
esEndpoint="http://"+sys.argv[1]+"/" 

indicesList=[] 
creationTimes=[] 
removeElements=[] 
########## 

########## MAIN CODE ########## 
print ("-------------------") 
print ("Starting program...\n") 

### First, get the list of all available indices... 
indices = requests.get(esEndpoint+"_cat/indices") 
result = indices.text.split('\n') 
del result[-1] 

for line in result: 
	indicesList.append(line.split()[2]) 

# remove the .kibana4 (or ".kibana" in ES 5.1) index from the list as it is a required/default index in ES 
if ".kibana-4" in indicesList: 
	indicesList.remove(".kibana-4") 
elif ".kibana" in indicesList: 
	indicesList.remove(".kibana") 

### Next, get all of the creation times for the various indices... 
for i in range (0,len(indicesList)): 
	cdates = requests.get(esEndpoint+indicesList[i]) 
	cdates2 = cdates.json() 
	creationTimes.append(cdates2[indicesList[i]]['settings']['index']['creation_date']) 

print ("\n\nEpoch Timestamps in human readable format are: ") 
print ("IndexName\t\tCreationTime (Epoch)\tCreationTime (Human Readable - UTC)") 
for i in range (0,len(creationTimes)): 
	print (indicesList[i]+": \t\t"+creationTimes[i]+"\t\t"+datetime.datetime.fromtimestamp(float(creationTimes[i]) / 1000).strftime('%Y-%m-%d %H:%M:%S')) 
print ("") 

### Next, determine which indices we should remove. Older than 31 days 
# for testing, tested older than 2 hours ago 
# test offset value (commented out) is the epoch value of 2 hours = 1000ms*60secs*60mins*2hours 
# offset = 7200000 

# offset here is set for 31 days -> 1000ms*60s*60m*24h*31d = 2678400000 
offset = (1000 * 60 * 60 * 24 * int(sys.argv[2])) 
currentTime = int(time.time() * 1000) 
checkTime = currentTime - offset 

# check the element values to see if they are outside the threshold time. Add the index element numbers to an array. 
for i in range (0, len(creationTimes)): 
	if checkTime > int(creationTimes[i]): 
		removeElements.append(i) 

# If there are no indices in the threshold time, exit the program - else continue on... 
print ("\nThreshold time (UTC) to check indices against is:") 
print (datetime.datetime.fromtimestamp(checkTime / 1000).strftime('%Y-%m-%d %H:%M:%S')) 
if (len(removeElements) != 0): 
	print ("Removing the indices...") 
	for element in removeElements: 
		print ("Removing index: "+indicesList[element]) 
		delete = requests.delete(esEndpoint+indicesList[element]) 
		print ("Index removed: "+indicesList[element]) 
	print("\nIndices successfully removed.") 
	print ("-------------------\n\n") 
else: 
	print ("\nThere are no indices that are older than the threshold time of: " + (datetime.datetime.fromtimestamp(checkTime / 1000).strftime('%Y-%m-%d %H:%M:%S'))) 
	print ("\nExiting program...") 
	print ("-------------------\n\n") 
	sys.exit(0) 


