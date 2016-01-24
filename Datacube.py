###################################################################
#
# Name : Surya Narayanan
# Topic : Aggregation of data cube.
# Task : 1.) List top 5 state-wise most popular job positions and
#		 2.) List top 5 most popular job titles in a given country
# Input	Files: apps.tsv, users.tsv, jobs.tsv,user_history.tsv
# Date : October 17, 2014
# For Python version: 2.7 
###################################################################

###################################################################
# Importing the libaries
###################################################################
import csv
import os, sys
import time
import datetime
import collections
from collections import defaultdict
from collections import Counter
from operator import itemgetter, attrgetter, methodcaller
###################################################################
# Reading the File Path
###################################################################
CountryForSlice=str(sys.argv[1])
filepath=str(sys.argv[3])
filepath1=str(sys.argv[2])
filepath2=str(sys.argv[4])
filepath3=str(sys.argv[5])
######################################################################################
# PROBLEM -1  Fetching the top 5 cells with Most Number Applications Grouped by State
######################################################################################
def TopStatewiseJobApplications(): 
	lines = []
	lines1 = []
	newlines7 = []
	FetchingKeysforCountry= []
	FetchingValuesforCountry = []
	FetchingCountOfValues = []
	ValuesOfMostpopularJobs =[]
	reader = csv.reader(open(filepath, "r"), delimiter='\t')
	reader1 = csv.reader(open(filepath1, "r"), delimiter='\t')
	iCountofUsers=0
	iCountofApps=0
	userID = []
###################################################################
# Reading the Users File into List
###################################################################	
	for line in reader:
		lines.append(line)
###################################################################
# Reading the Apps File into List
###################################################################		
	for line1 in reader1:
		lines1.append(line1)		
###################################################################
# Mapping the Key Value of Apps File into the Dictionary
###################################################################	
	d = collections.defaultdict(list)
	for UserID,TimeStamp,JobId in lines1:
		d[UserID].append(JobId)
###################################################################
# Mapping the Key Value of Users File into the Dictionary
###################################################################	
	d1 = collections.defaultdict(list)
	for UserId,City,State,Country,Zip,Degree,Major,Graddate,WorkHistory,TYE,Currently,ManagedOther,ManagedHowmany in lines:
		d1[UserId].append(State)
###################################################################
# Mapped Job Id to State
###################################################################	
	for key in d:
		List_Key = d1[key]
		iList_Key_1 = "".join(List_Key)
		if key in d1:
			list7 = d[key]
			for x in list7:
				newlines7.append([iList_Key_1,x])
###################################################################
# Dictionary which Mapps the JOB id to list of States Associated
###################################################################		
	d2 = defaultdict(list)
	for Country,JobID in newlines7:
		d2[JobID].append(Country)
###################################################################
# Fetching the Most Common For JobID for State Wise
###################################################################		
	FetchingKeysforCountry = d2.keys()
	for x in FetchingKeysforCountry:
		FetchingValuesforCountry = d2[x]
		most_common,num_most_common = Counter(FetchingValuesforCountry).most_common(1)[0]
		ValuesOfMostpopularJobs.append([x,most_common,num_most_common])		
###################################################################
# Ordering the Values Based on the No ofApps
###################################################################	
	ValuesOfMostpopularJobs =  sorted(ValuesOfMostpopularJobs, key=itemgetter(2), reverse=True)
####################################################################
# Printing the Value
####################################################################
	count = 0
	print "\n*****************************************************************************************"
	print " PROBLEM 1: \n"
	print " STATE	JOBID	NOOFAPPS " 
	print " =====	=====	========= " 
	for y in  ValuesOfMostpopularJobs:
		if count < 5 :
			iTemplength = 8 - len(y[0])
			iTempcount=0
			iSpaceTemp=" "
			while (iTempcount < iTemplength):
				iSpaceTemp = iSpaceTemp+ " " 
				iTempcount+=1
			print " ",y[1],"	",y[0],iSpaceTemp,y[2]
			count += 1
	print "\n*****************************************************************************************"
	
#############################################################################################
# End of Problem-1
#############################################################################################
	
#############################################################################################
# PROBLEM -2 etching the top 5 cells with Most Number Applications Grouped by State
#############################################################################################
def TopCountrywiseJobTitles(): 
	reader = csv.reader(open(filepath, "r"), delimiter='\t')
	reader1 = csv.reader(open(filepath1, "r"), delimiter='\t')
	reader2 = csv.reader(open(filepath2, "r"), delimiter='\t',quoting=csv.QUOTE_NONE)
	listofUsers = []
	listofJobs = []
	listofApps = []
	listforJobTitleStateID = []
	listforJobTitleStateID_1 = []
	listforJobIdStateId = []
	listforJobIdStateId_1 = []
	iCountofUsers=0
	iCountofJobs=0
	iCountofApps=0
	ValuesOfMostpopularJobTitles = []
	userID = []	
###################################################################
# Reading the Users File into List
###################################################################		
	for readingintousers in reader:
		listofUsers.append(readingintousers)		
###################################################################
# Reading the Apps File into List
###################################################################		
	for readingintojobapps in reader1:
		listofApps.append([readingintojobapps[0],readingintojobapps[2]])
###################################################################
# Reading the Jobs File into List
###################################################################	
	for readingintojobs in reader2:
		listofJobs.append([readingintojobs[0],readingintojobs[1]])		
###################################################################
# Mapping the Key Value of Users File into the Dictionary
###################################################################		
	dictionaryforusers = collections.defaultdict(list)
	for UserId,City,State,Country,Zip,Degree,Major,Graddate,WorkHistory,TYE,Currently,ManagedOther,ManagedHowmany in listofUsers:
		if Country == CountryForSlice:
			dictionaryforusers[UserId].append(State)	
###################################################################
# Mapping the Key Value of Apps File into the Dictionary
###################################################################			
	dictionaryforApps = collections.defaultdict(list)
	sTemp = 'JobID'
	for UserID,JobId in listofApps:
		if JobId != sTemp:
			dictionaryforApps[UserID].append(JobId)	
###################################################################
# Mapping the Key Value of Jobs File into the Dictionary
###################################################################		
	dictionaryforJobs = collections.defaultdict(list)
	for JobId,Title in listofJobs:
		if JobId != sTemp:
			dictionaryforJobs[JobId].append(Title)
##################################################################
# Mapping Job Id to State Id
##################################################################
	for key in dictionaryforApps:
		if key in dictionaryforusers:
			listforJobIdStateId = dictionaryforApps[key]
			value_for_dictionaryforApps = dictionaryforusers[key]
			value_for_dictionaryforApps_1 = "".join(value_for_dictionaryforApps)
			for samplevalue in listforJobIdStateId:
				listforJobIdStateId_1.append([value_for_dictionaryforApps_1,samplevalue])	
##################################################################
# Dictionary which Mapps the JOB id to the States Associated
##################################################################
	dictionaryforStatewiseJobs = collections.defaultdict(list)
	for State,JobId in  listforJobIdStateId_1:
		dictionaryforStatewiseJobs[JobId].append(State)
###############################################################
# Mapping the State ID to the Job Title
###############################################################
	for key in dictionaryforStatewiseJobs:
		if key in dictionaryforJobs:
			listforJobTitleStateID = dictionaryforStatewiseJobs[key]
			value_for_dictionaryforJobTitle = dictionaryforJobs[key]
			value_for_dictionaryforJobTitle_1 = "".join(value_for_dictionaryforJobTitle)
			for Itemsinjobs in listforJobTitleStateID:
				listforJobTitleStateID_1.append([value_for_dictionaryforJobTitle_1,Itemsinjobs])
###################################################################
# Dictionary which Mapps the JOB  Title to State
###################################################################
	dictionaryforJobTitleStateID = collections.defaultdict(list)
	for JobTitle,State in  listforJobTitleStateID_1:
		if len(State) > 0:
			dictionaryforJobTitleStateID[JobTitle].append(State)
		else:
			State = "None"
			dictionaryforJobTitleStateID[JobTitle].append(State)
############################################################
# Counting the Number of States for Each Country
############################################################
	for key, value in dictionaryforJobTitleStateID.items():
		ValuesOfMostpopularJobTitles.append([key, len([item for item in value if item])])
	ValuesOfMostpopularJobTitles =  sorted(ValuesOfMostpopularJobTitles, key=itemgetter(1), reverse=True)	
####################################################################
#Printing the Output
####################################################################
	count1 = 0 
	print "*****************************************************************************************"
	print " PROBLEM 2:\n"
	itempspaceforprint = "                            	 				"
	print " 	"+"TITLEID"+itempspaceforprint+"NOOFAPPS"
	print " 	"+"========"+itempspaceforprint+"========"

	for y in  ValuesOfMostpopularJobTitles:
		if count1 < 5 :
			#print " ",y[0],"				",y[1]
			iTemp=(81 - len(y[0]))
			iTemp1=0
			iTempSpace=" "
			while (iTemp1 < iTemp):
				iTempSpace = iTempSpace + " "
				iTemp1+=1
			print " ",y[0]+iTempSpace+str(y[1])
			count1 +=1
	print "\n*****************************************************************************************\n"
	
####################################################################
# Main Function
####################################################################
def main():
	TopStatewiseJobApplications()
	TopCountrywiseJobTitles()

if __name__ == "__main__":
	main()
####################################################################
# Exit program
####################################################################
sys.exit(0)

####################################################################
#End of Program 
####################################################################


####################################################################
# References 
####################################################################
#https://docs.python.org
#http://svn.python.org/projects/python/trunk/Lib/collections.py
####################################################################



