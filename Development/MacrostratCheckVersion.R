# Custom functions are camelCase. Arrays, parameters, and arguments are PascalCase
# Dependency functions are not embedded in master functions, and are marked with the flag dependency in the documentation
# []-notation is used wherever possible, and $-notation is avoided.

######################################### Load Required Libraries ###########################################
# Save and print the app start time
Start<-print(Sys.time())

# If running from UW-Madison
# Load or install the doParallel package
if (suppressWarnings(require("doParallel"))==FALSE) {
    install.packages("doParallel",repos="http://cran.cnr.berkeley.edu/");
    library("doParallel");
    }

# Load or install the RPostgreSQL package
if (suppressWarnings(require("RPostgreSQL"))==FALSE) {
    install.packages("RPostgreSQL",repos="http://cran.cnr.berkeley.edu/");
    library("RPostgreSQL");
    }

# Load or install the RCurl package (for Macrostrat check portion of app only)
if (suppressWarnings(require("RCurl"))==FALSE) {
    install.packages("RCurl",repos="http://cran.cnr.berkeley.edu/");
    library("RCurl");
    }

# Start a cluster for multicore, 3 by default or higher if passed as command line argument
CommandArgument<-commandArgs(TRUE)
if (length(CommandArgument)==0) {
     Cluster<-makeCluster(3)
     } else {
     Cluster<-makeCluster(as.numeric(CommandArgument[1]))
     }

#############################################################################################################
##################################### DATA DWONLOAD FUNCTIONS, FIDELITY #####################################
#############################################################################################################
# No funcitons at this time

########################################### Data Download Script ############################################
# print current status to terminal 
print(paste("Load postgres tables",Sys.time()))

# If RUNNING FROM UW-MADISON:
# Download the config file
Credentials<-as.matrix(read.table("Credentials.yml",row.names=1))
# Connect to PostgreSQL
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = Credentials["database:",], host = Credentials["host:",], port = Credentials["port:",], user = Credentials["user:",])
# Query the sentences fro postgresql
DeepDiveData<-dbGetQuery(Connection,"SELECT* FROM nlp_sentences_352") 

# IF TESTING IN 402:
# Download data from Postgres:
#Driver <- dbDriver("PostgreSQL") # Establish database driver
#Connection <- dbConnect(Driver, dbname = "labuser", host = "localhost", port = 5432, user = "labuser")
#DeepDiveData<-dbGetQuery(Connection,"SELECT* FROM pbdb_fidelity.pbdb_fidelity_data")

# Record initial stats
Description1<-"Initial Data"
# Initial number of documents and rows in DeepDiveData
Docs1<-length((unique(DeepDiveData[,"docid"])))
Rows1<-nrow(DeepDiveData)
Clusters1<-0

#############################################################################################################
###################################### DATA CLEANING FUNCTIONS, FIDELITY ####################################
#############################################################################################################
# No functions at this time

############################################ Data Cleaning Script ###########################################
# print current status to terminal
print(paste("Clean DeepDiveData",Sys.time()))

# Remove bracket symbols ({ and }) from DeepDiveData sentences
DeepDiveData[,"words"]<-gsub("\\{|\\}","",DeepDiveData[,"words"])

# Remove bracket symbols ({ and }) from DeepDiveData poses column
DeepDiveData[,"poses"]<-gsub("\\{|\\}","",DeepDiveData[,"poses"])

# Remove commas from DeepDiveData poses column
DeepDiveData[,"poses"]<-gsub(","," ",DeepDiveData[,"poses"])

# Remove commas from DeepDiveData to prepare to run grep function
CleanedDDWords<-gsub(","," ",DeepDiveData[,"words"])

#############################################################################################################
###################################### FORMATION SEARCH FUNCTIONS, FIDELITY #################################
#############################################################################################################

########################################### Formation Search Script #########################################
# print current status 
print(paste("Search for the word ' formation' in DeepDiveData sentences",Sys.time()))

# Apply grep to the object cleaned words
FormationHits<-parSapply(Cluster," formation",function(x,y) grep(x,y,ignore.case=TRUE, perl = TRUE),CleanedDDWords)
# Extact DeepDiveData rows corresponding with formation hits
SubsetDeepDive<-DeepDiveData[FormationHits,]

# Update the stats table
Description2<-"Subset DeepDiveData to rows which contain the word 'formation'"
# Record number of documents and rows in SubsetDeepDive:
Docs2<-length((unique(SubsetDeepDive[,"docid"])))
Rows2<-nrow(SubsetDeepDive)
Clusters2<-0

#############################################################################################################
####################################### NNP CLUSTER FUNCTIONS, FIDELITY #####################################
#############################################################################################################
# Consecutive word position locater function:
findConsecutive<-function(DeepDivePoses) {
    Breaks<-c(0,which(diff(DeepDivePoses)!=1),length(DeepDivePoses))
    ConsecutiveList<-lapply(seq(length(Breaks)-1),function(x) DeepDivePoses[(Breaks[x]+1):Breaks[x+1]])
    return(ConsecutiveList)
    }

############################################## NNP Cluster Script ###########################################
# Replace slashes from SubsetDeepDive words and poses columns with the word "SLASH"
SubsetDeepDive[,"words"]<-gsub("\"","SLASH",SubsetDeepDive[,"words"])
SubsetDeepDive[,"poses"]<-gsub("\"","SLASH",SubsetDeepDive[,"poses"])

# print current status to terminal
print(paste("Extract NNPs from SubsetDeepDive rows",Sys.time()))

# Create a list of vectors showing each formation hit sentence's unlisted poses column 
DeepDivePoses<-parSapply(Cluster, SubsetDeepDive[,"poses"],function(x) unlist(strsplit(as.character(x)," ")))
# Assign names to each list element corresponding to the document and sentence id of each sentence
doc.sent<-paste(SubsetDeepDive[,"docid"],SubsetDeepDive[,"sentid"],sep=".")
names(DeepDivePoses)<-doc.sent

# Extract all the NNPs from DeepDivePoses
# NOTE: Search for CC as to get hits like "Middendorf And Black Creek Formations" which is NNP, CC, NNP, NNP, NNP
DeepDiveNNPs<-parSapply(Cluster, DeepDivePoses,function(x) which(x=="NNP"|x=="CC"))
    
# print current status to terminal
print(paste("Find consecutive NNPs in DeepDiveNNPs",Sys.time()))
    
# Apply function to DeepDiveNNPs list
ConsecutiveNNPs<-sapply(DeepDiveNNPs, findConsecutive)   
# Collapse each cluster into a single character string such that each sentence from formation hits shows its associated clusters    
SentenceNNPs<-sapply(ConsecutiveNNPs,function(y) sapply(y,function(x) paste(x,collapse=",")))
    
# print current status to terminal
print(paste("Find words Associated with Conescutive NNPs",Sys.time()))
    
# Create a data frame with a row for each NNP cluster
# Make a column for cluster elements 
ClusterPosition<-unlist(SentenceNNPs)
# Make a column for sentence IDs
ClusterCount<-sapply(SentenceNNPs,length)
# Repeat the document & sentence ID info (denoted in the names of SentenceNNPs) by the number of NNP clusters in each sentence
DocSentID<-rep(names(SentenceNNPs),times=ClusterCount)
SplitDocSent<-strsplit(DocSentID,'\\.') 
# Create docid column for each cluster
docid<-sapply(SplitDocSent,function(x) x[1])
# make a sentid column for each cluster
sentid<-as.numeric(sapply(SplitDocSent,function(x) x[2]))    
# Bind cluster position data with document/sentence id data
ClusterData<-as.data.frame(cbind(ClusterPosition,docid,sentid))
# Remove NA's from ClusterData
ClusterData<-ClusterData[which(ClusterData[,"ClusterPosition"]!="NA"),]
# Reformat ClusterData
ClusterData[,"ClusterPosition"]<-as.character(ClusterData[,"ClusterPosition"])
ClusterData[,"docid"]<-as.character(ClusterData[,"docid"])
ClusterData[,"sentid"]<-as.numeric(as.character(ClusterData[,"sentid"]))
    
# Make a column for the words associated with each NNP
# Create a vector of the number of rows in ClusterData.
NumClusterVector<-1:nrow(ClusterData)   
# Extract the proper SubsetDeepDive rows based on the data in ClusterData    
SubsetDeepDiveRow<-parSapply(Cluster, NumClusterVector,function(x,y,z) which(y[,"docid"]==z[x,"docid"]&y[,"sentid"]==z[x,"sentid"]), SubsetDeepDive, ClusterData)
# Bind row data to ClusterData and convert it into a dataframe
ClusterData<-cbind(ClusterData,SubsetDeepDiveRow)
ClusterData[,"SubsetDeepDiveRow"]<-as.numeric(as.character(ClusterData[,"SubsetDeepDiveRow"]))
 
# Extract the sentences the associated SubsetDeepDive rows  
ClusterSentences<-sapply(ClusterData[,"SubsetDeepDiveRow"], function (x) SubsetDeepDive[x,"words"])
# Split and unlist the words in each cluster sentence
ClusterSentencesSplit<-sapply(ClusterSentences,function(x) unlist(strsplit(as.character(x),",")))
# Extract the NNP Clusters from theh associate sentences 
# Get numeric elements for each NNP Cluster word
NNPElements<-lapply(ClusterData[,"ClusterPosition"],function(x) as.numeric(unlist(strsplit(x,","))))
# Create a vector for the number of Clusters in ClusterData
NumClusterVector<-1:nrow(ClusterData) 
# Extract the words from ClusterSentencesSplit       
ClusterWords<-sapply(NumClusterVector, function(y) sapply(NNPElements[y], function(x) ClusterSentencesSplit[[y]][x]))
# Collapse the clusters into single character strings
NNPWords<-sapply(ClusterWords, function(x) paste(array(x), collapse=" "))
# Bind the clusters to the ClusterData frame
ClusterData[,"NNPWords"]<-NNPWords
    
# Update the stats table
Description3<-"Extract NPP clusters from SubsetDeepDive rows"
# Record number of documents and rows in SubsetDeepDive:
Docs3<-length(unique(ClusterData[,"docid"]))
Rows3<-length(unique(ClusterData[,"SubsetDeepDiveRow"]))
Clusters3<-nrow(ClusterData)

#############################################################################################################
##################################### FORMATION CLUSTERS FUNCTIONS, FIDELITY ################################
#############################################################################################################    
# Capitalization function from stack exchane
simpleCap <- function(x) {
  s <- strsplit(x, " ")[[1]]
  paste(toupper(substring(s, 1,1)), substring(s, 2),
      sep="", collapse=" ")
}
    
########################################### Formation Clusters Script #######################################
# print current status to terminal
print(paste("Extract 'formation' clusters from ClusterData",Sys.time()))
    
# Find NNP clusters with the world formation in them
FormationClusters<-grep(" formation",ClusterData[,"NNPWords"],ignore.case=TRUE,perl=TRUE) # We could do a search for tail, to ensure it's the last word
# Extract those rows from ClusterData
FormationData<-ClusterData[FormationClusters,]
FormationData[,"docid"]<-as.character(FormationData[,"docid"])
    
# Update the stats table
Description4<-"Extract NNP clusters containing the word 'formation'"
# Record number of documents and rows in SubsetDeepDive:
Docs4<-length(unique(FormationData[,"docid"]))
Rows4<-length(unique(FormationData[,"SubsetDeepDiveRow"]))
Clusters4<-nrow(FormationData)
    
# print current status to terminal
print(paste("Capitalize formation names appropriately",Sys.time()))
    
# Make all characters in the NNPWords column lower case
FormationData[,"NNPWords"]<-tolower(FormationData[,"NNPWords"])
# Apply simpleCap function to NNPWords column so the first letter of every word is capitalized.
FormationData[,"NNPWords"]<-sapply(FormationData[,"NNPWords"], simpleCap)
    
# print current status to terminal
print(paste(" Remove all characters after 'Formation' or 'Formations'",Sys.time()))
    
# Account for romance language exceptions
Des<-grep("Des",FormationData[,"NNPWords"], perl=TRUE, ignore.case=TRUE)
Les<-grep("Les",FormationData[,"NNPWords"], perl=TRUE, ignore.case=TRUE)
FrenchRows<-c(Des,Les)
    
# Extract FormationData NNPWords with "Formations" NNP clusters
PluralWithFrench<-grep("Formations",FormationData[,"NNPWords"], perl=TRUE, ignore.case=TRUE)
# Make sure character removal is not performed on french rows
Plural<-PluralWithFrench[which(!PluralWithFrench%in%FrenchRows)]
# Replace (non-french) plural rows of NNPWords column with version with characters after "formations" removed
FormationsCut<-gsub("(Formations).*","\\1",FormationData[Plural,"NNPWords"])
FormationData[Plural,"NNPWords"]<-FormationsCut
    
# Extract FormationData NNPWords with "Formation" NNP clusters
# Find the FormationData NNPWords rows with "Formation" NNP clusters (NON PLURALS)
SingularWithFrench<-which(!1:nrow(FormationData)%in%Plural)
# Make sure character removal is not performed on french rows
Singular<-SingularWithFrench[which(!SingularWithFrench%in%FrenchRows)]
# Replace (non-french) singular rows of NNPWords column with version with characters after "formation" removed
FormationCut<-gsub("(Formation).*","\\1",FormationData[Singular,"NNPWords"])
FormationData[Singular,"NNPWords"]<-FormationCut
    
# Remove FormationData rows which only have "Formation" in the NNPWords column
FormationData<-FormationData[-which(FormationData[,"NNPWords"]=="Formation"),]
 
# Update the stats table
Description5<-"Remove rows that are just the word 'Formation'"
# Record number of documents and rows in SubsetDeepDive:
Docs5<-length(unique(FormationData[,"docid"]))
Rows5<-length(unique(FormationData[,"SubsetDeepDiveRow"]))
Clusters5<-nrow(FormationData)        
       
# STEP THIRTEEN: Split the NNPClusters where there is an "And"
SplitFormations<-strsplit(FormationData[,"NNPWords"],'And ')
# Remove the blanks created by the splitting
SplitFormationsClean<-sapply(SplitFormations,function(x) unlist(x)[unlist(x)!=""])   
# SplitFormations is a list of the split clusters. Figure out which clusters were split at "And" using length.
SplitCount<-sapply(SplitFormationsClean,length)
# Repeat the data in FormationData for each split cluster by its length
SubsetDeepDiveRow<-rep(FormationData[,"SubsetDeepDiveRow"],time=SplitCount)
ClusterPosition<-rep(FormationData[,"ClusterPosition"],times=SplitCount) 
docid<-rep(FormationData[,"docid"],times=SplitCount) 
sentid<-rep(FormationData[,"sentid"],times=SplitCount)
# Make a column for the split formations
Formation<-unlist(SplitFormationsClean)
FormationData<-as.data.frame(cbind(Formation,SubsetDeepDiveRow,ClusterPosition,docid,sentid))
# Reformat data
FormationData[,"SubsetDeepDiveRow"]<-as.numeric(as.character(FormationData[,"SubsetDeepDiveRow"]))
FormationData[,"Formation"]<-as.character(FormationData[,"Formation"])
FormationData[,"ClusterPosition"]<-as.character(FormationData[,"ClusterPosition"])
FormationData[,"docid"]<-as.character(FormationData[,"docid"])
FormationData[,"sentid"]<-as.numeric(as.character(FormationData[,"sentid"]))

# Paste "Formation" to the end of the split clusters where it is missing
# Determine the split clusters that DO contain the word "Formation"
FormationHalves<-grep("Formation",FormationData[,"Formation"], perl=TRUE, ignore.case=TRUE)
# Paste "Formation" to all of the non FormationHalves rows
FormationData[-FormationHalves,"Formation"]<-paste(FormationData[-FormationHalves,"Formation"], "Formation", sep=" ")
    
# Update the stats table
Description6<-"Split NNPClusters at 'And'"
# Record number of documents and rows in SubsetDeepDive:
Docs6<-length(unique(FormationData[,"docid"]))
Rows6<-length(unique(FormationData[,"SubsetDeepDiveRow"]))
Clusters6<-nrow(FormationData)
  
# STEP FOURTEEN: Remove Formations that equal to 1 word in length or more than 5 words in length.
print(paste("Remove Formations > 5 or = 1 word(s) in length",Sys.time()))
# Determine the number of words in each NNPWords row
WordLength<-sapply(sapply(FormationData[,"ClusterPosition"], function(x) strsplit(x, ",")), function(x) length(x))
# Determine which rows have more than 5 NNPWords or only 1 NNPWord
BadFormations<-which(WordLength>5|WordLength==1)
# Remove those rows from FormationData
FormationData<-FormationData[-BadFormations,]

# Update the stats table
Description7<-"Remove Formations > 5 words in length"
# Record number of documents and rows in SubsetDeepDive:
Docs7<-length(unique(FormationData[,"docid"]))
Rows7<-dim(unique(FormationData[,c("docid","sentid")]))[1]
Clusters7<-nrow(FormationData) 

# STEP FIFTEEN: Clean FormationData
print(paste("Clean FormationData",Sys.time()))
# Remove spaces at the beginning and/or end of the Formation column where necessary
FormationData[,"Formation"]<-trimws(FormationData[,"Formation"], which=c("both"))
# Remove double spaces in the formation column
FormationData[,"Formation"]<-gsub("  "," ",FormationData[,"Formation"])
# Remove s in "Formations" where necessary
FormationData[,"Formation"]<-gsub("Formations","Formation",FormationData[,"Formation"])

    
#############################################################################################################
####################################### LOCATIONA MATCHING FUNCTIONS, FIDELITY ##############################
#############################################################################################################       
# No functions at this time 
    
########################################### Locations Matching Script #######################################     
print(paste("Search for age clusters",Sys.time()))
# Find non-formation clusters
PostFmClusters<-ClusterData[-FormationClusters,]

# Search for geologic time interval names in clusters
TimeURL<-getURL("https://macrostrat.org/api/defs/intervals?all&format=csv")
Timescales<-read.csv(text=TimeURL)
# Extract unique interval names
Intervals<-as.character(unique(Timescales[,"name"]))

# Find NNP clusters containing interval names
IntervalClusters<-parSapply(Cluster, Intervals, function(x,y) which(x==y), PostFmClusters[,"NNPWords"])
IntervalData<-PostFmClusters[unique(unlist(IntervalClusters)),]
  
# Collapse duplicate SubsetDeepDiveRows
# Extract unique SubsetDeepDiveRows 
SubRow<-unique(IntervalData[,"SubsetDeepDiveRow"])
# Locate duplicate rows
DuplicateIntervals<-parSapply(Cluster, SubRow, function(x,y) which(y[,"SubsetDeepDiveRow"]==x), IntervalData)
# Extract and collapse intervals for duplicates
CollapsedIntervals<-sapply(DuplicateIntervals, function(x,y) paste(unique(y[x,"NNPWords"]),collapse=","), IntervalData)
# Bind row location data to collapsed intervals
IntervalRows<-as.data.frame(cbind(SubRow,CollapsedIntervals))
# Reformat IntervalRows
IntervalRows[,"SubRow"]<-as.numeric(as.character(IntervalRows[,"SubRow"]))
# Merge interval rows to IntervalData by SubsetDeepDiveRow
IntervalData<-merge(IntervalData, IntervalRows, by.x="SubsetDeepDiveRow", by.y="SubRow", all.x=TRUE)      
IntervalData<-unique(IntervalData[,c("docid","sentid","SubsetDeepDiveRow","CollapsedIntervals")])

print(paste("Search for country clusters",Sys.time()))
# Find non-formation, non-age clusters
PostAgeClusters<-PostFmClusters[-unique(unlist(IntervalClusters)),]

# NOTE: IN OFFICIAL APP CONSIDER SEARCHING FOR OFFICIAL / ALTERNATIVE LOCATION NAMES
# Search for countries in clusters
# Load location data
# If testing in 402: WorldCities<-read.csv("~/Documents/DeepDive/International_Formations/MacrostratTesting/world_cities_province.csv")
WorldCities<-read.csv("input/world_cities_province.csv")
# Extract unique country names
Countries<-unique(WorldCities[,"wc_country"])
CountryClusters<-parSapply(Cluster, Countries, function(x,y) which(x==y), PostAgeClusters[,"NNPWords"])
CountryData<-PostAgeClusters[unique(unlist(CountryClusters)),]
    
# Collapse duplicate SubsetDeepDiveRows
# Extract unique SubsetDeepDiveRows 
SubRow<-unique(CountryData[,"SubsetDeepDiveRow"])
# Locate duplicate rows
DuplicateCountries<-parSapply(Cluster, SubRow, function(x,y) which(y[,"SubsetDeepDiveRow"]==x), CountryData)
# Extract and collapse countries for duplicates
CollapsedCountries<-sapply(DuplicateCountries, function(x,y) paste(unique(y[x,"NNPWords"]),collapse=","), CountryData)
# Bind row location data to collapsed countries
CountryRows<-as.data.frame(cbind(SubRow,CollapsedCountries))
# Reformat IntervalRows
CountryRows[,"SubRow"]<-as.numeric(as.character(CountryRows[,"SubRow"]))
# Merge interval rows to CountryData by SubsetDeepDiveRow
CountryData<-merge(CountryData, CountryRows, by.x="SubsetDeepDiveRow", by.y="SubRow", all.x=TRUE)      
CountryData<-unique(CountryData[,c("docid","sentid","SubsetDeepDiveRow","CollapsedCountries")])

print(paste("Search for admin clusters",Sys.time()))
# Find non-formation, non-age, non-country clusters
PostCountryClusters<-PostAgeClusters[-unique(unlist(CountryClusters)),]
    
# Search for province/state names in clusters
Admins<-unique(WorldCities[,"woe_name"])
AdminClusters<-parSapply(Cluster, Admins, function(x,y) which(x==y), PostCountryClusters[,"NNPWords"])
AdminData<-PostCountryClusters[unique(unlist(AdminClusters)),]
    
# Collapse duplicate SubsetDeepDiveRows
# Extract unique SubsetDeepDiveRows 
SubRow<-unique(AdminData[,"SubsetDeepDiveRow"])
# Locate duplicate rows
DuplicateAdmins<-parSapply(Cluster, SubRow, function(x,y) which(y[,"SubsetDeepDiveRow"]==x), AdminData)
# Extract and collapse admins for duplicates
CollapsedAdmins<-sapply(DuplicateAdmins, function(x,y) paste(unique(y[x,"NNPWords"]),collapse=","), AdminData)
# Bind row location data to collapsed admins
AdminRows<-as.data.frame(cbind(SubRow,CollapsedAdmins))
# Reformat AdminRows
AdminRows[,"SubRow"]<-as.numeric(as.character(AdminRows[,"SubRow"]))
# Merge interval rows to AdminData by SubsetDeepDiveRow
AdminData<-merge(AdminData, AdminRows, by.x="SubsetDeepDiveRow", by.y="SubRow", all.x=TRUE)      
AdminData<-unique(AdminData[,c("docid","sentid","SubsetDeepDiveRow","CollapsedAdmins")])

print(paste("Search for city clusters",Sys.time()))
# Find non-formation, non-age, non-country, non-admin clusters
PostAdminClusters<-PostCountryClusters[-unique(unlist(AdminClusters)),]
    
# Search for city names in clusters
Cities<-as.character(unique(WorldCities[,"city_name"]))
# Remove formation names that are identical to city names from the search
BadCities<-gsub( " Formation", "", FormationData[,"Formation"])
CleanCities<-Cities[which(Cities%in%BadCities==FALSE)]
    
CityClusters<-parSapply(Cluster, CleanCities, function(x,y) which(x==y), PostAdminClusters[,"NNPWords"])
CityData<-PostAdminClusters[unique(unlist(CityClusters)),]
    
# Collapse duplicate SubsetDeepDiveRows
# Extract unique SubsetDeepDiveRows 
SubRow<-unique(CityData[,"SubsetDeepDiveRow"])
# Locate duplicate rows
DuplicateCities<-parSapply(Cluster, SubRow, function(x,y) which(y[,"SubsetDeepDiveRow"]==x), CityData)
# Extract and collapse admins for duplicates
CollapsedCities<-sapply(DuplicateCities, function(x,y) paste(unique(y[x,"NNPWords"]),collapse=","), CityData)
# Bind row location data to collapsed admins
CityRows<-as.data.frame(cbind(SubRow,CollapsedCities))
# Reformat AdminRows
CityRows[,"SubRow"]<-as.numeric(as.character(CityRows[,"SubRow"]))
# Merge interval rows to AdminData by SubsetDeepDiveRow
CityData<-merge(CityData, CityRows, by.x="SubsetDeepDiveRow", by.y="SubRow", all.x=TRUE)      
CityData<-unique(CityData[,c("docid","sentid","SubsetDeepDiveRow","CollapsedCities")])
    
PostCityClusters<-PostAdminClusters[-unique(unlist(CityClusters)),]
    
# STEP SIXTEEN: Merge other cluster data (age, location) into FormationData table
FormationData<-merge(FormationData, IntervalData[,c("SubsetDeepDiveRow", "CollapsedIntervals")], by="SubsetDeepDiveRow", all.x=TRUE)
FormationData<-merge(FormationData, CountryData[,c("SubsetDeepDiveRow", "CollapsedCountries")], by="SubsetDeepDiveRow", all.x=TRUE)
FormationData<-merge(FormationData, AdminData[,c("SubsetDeepDiveRow", "CollapsedAdmins")], by="SubsetDeepDiveRow", all.x=TRUE)
FormationData<-merge(FormationData, CityData[,c("SubsetDeepDiveRow", "CollapsedCities")], by="SubsetDeepDiveRow", all.x=TRUE)

# Assign column names
colnames(FormationData)<-c("SubsetDeepDiveRow","Formation","ClusterPosition","docid","sentid","age","country","admin","city")
    
# STEP SEVENTEEN: 

    
    
    
    
    
    
    
    
    
# STEP EIGHTEEN: Search for locations that oc-occur in sentences with formations.    
print(paste("Search for locations in FormationData sentences",Sys.time()))   
 
# Subset to only include cities in the U.S.
WorldCities<-WorldCities[which(WorldCities[,"wc_country"]=="United States"),]
# Extract unique cities in the United States
Cities<-unique(WorldCities[,c("city_name","woe_name","wc_latitude","wc_longitude")])
# Assign column names
colnames(Cities)<-c("city","state","latitude","longitude")
# Create unique character strings of city|state name and locations (based on lat long)
CityState<-apply(Cities, 1, function(x) paste(x, collapse="|"))
# Add a space at the end of all city names and admin titles to improve grep accuracy
Cities[,"city"]<-paste(Cities[,"city"]," ",sep="")
    
# Extract FormationData SubsetDeepDive rows for grep search
FormationSentences<-sapply(FormationData[,"SubsetDeepDiveRow"], function(x) SubsetDeepDive[x,"words"])
# Only search sentences which are less than or equal to 350 characters in length
ShortSentences<-which(sapply(FormationSentences, function(x) nchar(x)<=350))
FormationSentences<-FormationSentences[ShortSentences]
# Subset FormationData to only include short sentences
SubsetFormData<-FormationData[ShortSentences,]
    
# Clean the sentences to prepare for grep
CleanedWords<-gsub(","," ",FormationSentences)
# Replace all periods in CleanedWords with spaces to avoid grep errors
CleanedWords<-gsub("\\."," ",CleanedWords)
    
# Search for cities: 
CityHits<-parSapply(Cluster, Cities[,"city"], function(x,y) grep(x, y, perl=TRUE, ignore.case=FALSE), CleanedWords)    
# Assign names
names(CityHits)<-CityState
# Determine which cities had matches 
CityCheck<-sapply(CityHits, function(x) length(x)>0)
# Extract those cities and their match data
CityMatches<-CityHits[which(CityCheck==TRUE)]
# Extract sentences with cities in them, and their docid, sent id data
CitySentence<-sapply(unlist(CityMatches), function(x) CleanedWords[x])
CitySentid<-sapply(unlist(CityMatches), function(x) SubsetFormData[x,"sentid"])
CityDocid<-sapply(unlist(CityMatches), function(x) SubsetFormData[x,"docid"])
CitySentid<-sapply(unlist(CityMatches), function(x) SubsetFormData[x,"sentid"])
# Extract the formation associated with that city
CityFormation<-sapply(unlist(CityMatches), function(x) SubsetFormData[x,"Formation"])
# Create a city name column
CityCount<-sapply(CityMatches, length)
CityStateName<-rep(names(CityMatches), times=CityCount)
# Split CityCountryName back into separated cities and countries  
CityStateNameSplit<-sapply(CityStateName, function(x) strsplit(x,'\\|'))
# Create a CityName column
CityName<-sapply(CityStateNameSplit, function(x) x[1])
# Create a state column
state<-sapply(CityStateNameSplit, function(x) x[2])
# Create a latitude column
latitude<-sapply(CityStateNameSplit, function(x) as.numeric(x[3]))
# Create a longitude column
longitude<-sapply(CityStateNameSplit, function(x) as.numeric(x[4]))
# Bind this data into a dataframe
CityData<-as.data.frame(cbind(CityName,latitude ,longitude ,state ,CityFormation,CityDocid,CitySentid,CitySentence))
# Reformat CityData
CityData[,"CityName"]<-as.character(CityData[,"CityName"])
CityData[,"latitude"]<-as.numeric(as.character(CityData[,"latitude"]))
CityData[,"longitude"]<-as.numeric(as.character(CityData[,"longitude"]))
CityData[,"state"]<-as.character(CityData[,"state"])   
CityData[,"CityFormation"]<-as.character(CityData[,"CityFormation"])
CityData[,"CityDocid"]<-as.character(CityData[,"CityDocid"])
CityData[,"CitySentid"]<-as.numeric(as.character(CityData[,"CitySentid"]))
colnames(CityData)<-c("CityName","latitude","longitude","state","Formation","docid","sentid","Sentence")
rownames(CityData)<-NULL
    
# Subset DeepDiveData to only include documents in CityData table
LocationDeepDive<-subset(DeepDiveData, DeepDiveData[,"docid"]%in%CityData[,"docid"])
# Clean LocationDeepDive words column to prepare for grep
CleanedLocationWords<-gsub(","," ",LocationDeepDive[,"words"])
# Search for state names in CleanedLocationWords
StateHits<-parSapply(Cluster,unique(CityData[,"state"]),function(x,y) grep(x,y,ignore.case=FALSE, perl = TRUE),CleanedLocationWords)
# Extract the documents each state is found in 
StateDocs<-sapply(StateHits, function(x) unique(LocationDeepDive[x,"docid"]))
    
# Create a matrix of tuples of states and docids
# Make a state name column for tuples matrix    
StatesLength<-sapply(StateDocs, length)
state<-rep(names(StateDocs),times=StatesLength)   
# Make a docid column for tuples
docid<-unlist(StateDocs)
# Bind state and docid data
StateTuples<-cbind(state,docid)

# Add collapsed state, docid tuples to both CityData and StateTuples
CandidateStateDocs<-apply(CityData[,c("state","docid")], 1, function(x) paste(x, collapse="|"))
CityData<-cbind(CityData,CandidateStateDocs)

MatchedStateDocs<-apply(StateTuples, 1, function(x) paste(x, collapse="|"))
StateTuples<-cbind(StateTuples,MatchedStateDocs)
    
# Verify that the formation/city match is correct by making sure that the state/docid tuple in CityData is also found in StateTuples
CleanedCityData<-CityData[which(CityData[,"CandidateStateDocs"]%in%StateTuples[,"MatchedStateDocs"]),]  

# Download and check Macrostrat location data 
# Download all unit names from Macrostrat Database
UnitsURL<-paste("https://macrostrat.org/api/units?&project_id=1&response=long&format=csv")
GotURL<-getURL(UnitsURL)
UnitsFrame<-read.csv(text=GotURL,header=TRUE)

# Download all units from Macrostrat database at the formation level
StratURL<-"https://macrostrat.org/api/defs/strat_names?rank=fm&format=csv"
StratURL<-getURL(StratURL)
StratFrame<-read.csv(text=StratURL,header=TRUE)
    
# Load intersected location tuples table 
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = "labuser", host = "localhost", port = 5432, user = "labuser")
LocationTuples<-dbGetQuery(Connection,"SELECT* FROM column_locations.intersections") 

# Collapse all locations into one charcter string for each col_id in LocationTuples
# Extract unique col_ids
ColID<-unique(LocationTuples[,"col_id"])
ColIDLocations<-sapply(ColID, function(x) paste(LocationTuples[which(LocationTuples[,"col_id"]==x),"location"], collapse=" "))    
# Bind data
LocationTuplesCollapsed<-as.data.frame(cbind(ColID,ColIDLocations))
# Assign column names
colnames(LocationTuplesCollapsed)<-c("col_id","location")
 
# Make sure columns are formatted correctly for merge
LocationTuplesCollapsed[,"col_id"]<-as.numeric(as.character(LocationTuplesCollapsed[,"col_id"]))
LocationTuplesCollapsed[,"location"]<-as.character(LocationTuplesCollapsed[,"location"]) 
UnitsFrame[,"col_id"]<-as.numeric(as.character(UnitsFrame[,"col_id"]))
    
# merge the Location to UnitsFrame by col_id
UnitsFrame<-merge(UnitsFrame,LocationTuplesCollapsed,by="col_id", all.x=TRUE)
    
# Subset UnitsFrame to only include formations
UnitsFrame<-subset(UnitsFrame,UnitsFrame[,"strat_name_long"]%in%StratFrame[,"strat_name_long"])
# Paste the word "Formation" to each UnitsFrame[,"Fm"] column
UnitsFrame[,"Fm"]<-paste(UnitsFrame[,"Fm"], "Formation", sep=" ")
    
# Subset CityData to only formations found in Macrostrat
MacroCityData<-CleanedCityData[which(CleanedCityData[,"Formation"]%in%UnitsFrame[,"Fm"]),]
# Subset UnitsFrame to only include formation names from CleanedCityData
SubsetUnitsFrame<-subset(UnitsFrame, UnitsFrame[,"Fm"]%in%MacroCityData[,"Formation"])
      
# Check CityData locations for formations against Macrostrat formation locations
# From CityData:
CityDataFormationState<-MacroCityData[,c("Formation","state")]
# From Macrostrat:
MacroFormationState<-SubsetUnitsFrame[,c("Fm","location")]    

# Split all of the MacroFormationState locations
SplitMacroStates<-sapply(MacroFormationState[,"location"], function(x) strsplit(x, ' '))
# Assign associated formation names to split states
names(SplitMacroStates)<-MacroFormationState[,"Fm"]
# Find the number of locations for each formation name
LengthMacroStates<-sapply(SplitMacroStates, length)
# Make a unique formation name column for MacroFormationState data 
MacroFormations<-rep(names(SplitMacroStates), times=LengthMacroStates)    
# Make a unique location name column for MacroFormationState data
MacroStates<-unlist(SplitMacroStates)
# Bind formation and locatioin data from Macrostrat
MacroFormationState<-cbind(MacroFormations,MacroStates)
# Assign column names
colnames(MacroFormationState)<-c("Formation","state")
# Remove row names
rownames(MacroFormationState)<-NULL
# Remove duplicate data
MacroFormationState<-unique(MacroFormationState)
    
# Add collapsed formation|location columns to both MacroFormationState and CityDataFormationState matrices
MacroPaste<-apply(MacroFormationState, 1, function(x) paste(x, collapse="|"))
CityDataPaste<-apply(CityDataFormationState, 1, function(x) paste(x, collapse="|"))
# Bind data to MacroFormationState and CityDataFormationState
MacroFormationState<-cbind(MacroFormationState,MacroPaste)
CityDataFormationState<-cbind(CityDataFormationState,CityDataPaste)

# Check to see which formation,location tuples from CityDataFormationState appear in MacroFormationState
CheckedCityData<-MacroCityData[which(CityDataFormationState[,"CityDataPaste"]%in%MacroFormationState[,"MacroPaste"]),]    
    
    
    
