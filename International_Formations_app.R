Start<-print(Sys.time())

if (require("doParallel",warn.conflicts=FALSE)==FALSE) {
    install.packages("doParallel",repos="http://cran.cnr.berkeley.edu/");
    library("doParallel");
    }

if (require("RPostgreSQL",warn.conflicts=FALSE)==FALSE) {
    install.packages("RPostgreSQL",repos="http://cran.cnr.berkeley.edu/");
    library("RPostgreSQL");
    }

# Start a cluster for multicore, 3 by default or higher if passed as command line argument
CommandArgument<-commandArgs(TRUE)
if (length(CommandArgument)==0) {
     Cluster<-makeCluster(3)
     } else {
     Cluster<-makeCluster(as.numeric(CommandArgument[1]))
     }

# STEP ONE: Load DeepDiveData 
print(paste("Load postgres tables",Sys.time()))

# Download the config file
Credentials<-as.matrix(read.table("Credentials.yml",row.names=1))

# Connet to PostgreSQL
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = Credentials["database:",], host = Credentials["host:",], port = Credentials["port:",], user = Credentials["user:",])
# Make SQL query
DeepDiveData<-dbGetQuery(Connection,"SELECT* FROM nlp_sentences_352") 

# If Testing: 
#Driver <- dbDriver("PostgreSQL") # Establish database driver
#Connection <- dbConnect(Driver, dbname = "labuser", host = "localhost", port = 5432, user = "labuser")
#DeepDiveData<-dbGetQuery(Connection,"SELECT* FROM pbdb_fidelity.pbdb_fidelity_data")

# RECORD INITIAL STATS
# INITIAL NUMBER OF DOCUMENTS AND ROWS IN DEEPDIVEDATA: 
StepOneDescription<-"Initial Data"
# INITIAL NUMBER OF DOCUMENTS AND ROWS IN DEEPDIVEDATA:
StepOneDocs<-length((unique(DeepDiveData[,"docid"])))
StepOneRows<-nrow(DeepDiveData)
StepOneClusters<-0

# STEP TWO: Clean DeepDiveData 
print(paste("Clean DeepDiveData",Sys.time()))

# Remove bracket symbols ({ and }) from DeepDiveData sentences
DeepDiveData[,"words"]<-gsub("\\{|\\}","",DeepDiveData[,"words"])
# Remove bracket symbols ({ and }) from DeepDiveData poses column
DeepDiveData[,"poses"]<-gsub("\\{|\\}","",DeepDiveData[,"poses"])
# Remove commas from DeepDiveData to prepare to run grep function
CleanedWords<-gsub(","," ",DeepDiveData[,"words"])
# Remove commas from DeepDiveData poses column
DeepDiveData[,"poses"]<-gsub(","," ",DeepDiveData[,"poses"])

# STEP THREE: Search for the word " formation" in all cleaned DeepDiveData sentences (CleanedWords)
print(paste("Search for the word ' formation' in DeepDiveData sentences",Sys.time()))
# Apply grep to cleaned words
FormationHits<-parSapply(Cluster," formation",function(x,y) grep(x,y,ignore.case=TRUE, perl = TRUE),CleanedWords)

# STEP FOUR: Extact DeepDiveData rows corresponding with formation hits
print(paste("Extract formation hit rows from DeepDiveData",Sys.time()))
SubsetDeepDive<-sapply(FormationHits,function(x) DeepDiveData[x,])
# If testing: SubsetDeepDive<-sapply(FormationHits[1:1000],function(x) DeepDiveData[x,])
# Reformat SubsetDeepDive
SubsetDeepDive<-t(SubsetDeepDive)
    
# RECORD STATS
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE: 
StepFourDescription<-"Subset DeepDiveData to rows which contain the word 'formation'"
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE:
StepFourDocs<-length((unique(SubsetDeepDive[,"docid"])))
StepFourRows<-nrow(SubsetDeepDive)
StepFourClusters<-0

# STEP FIVE: Replace slashes from SubsetDeepDive words and poses columns with the word "SLASH"
print(paste("Clean SubsetDeepDive",Sys.time()))
SubsetDeepDive[,"words"]<-gsub("\"","SLASH",SubsetDeepDive[,"words"])
SubsetDeepDive[,"poses"]<-gsub("\"","SLASH",SubsetDeepDive[,"poses"])

# STEP SIX: Extract NNPs from SubsetDeepDive
print(paste("Extract NNPs from SubsetDeepDive rows",Sys.time()))
# Create a list of vectors showing each formation hit sentence's unlisted poses column 
DeepDivePoses<-sapply(SubsetDeepDive[,"poses"],function(x) unlist(strsplit(as.character(x)," ")))
# Assign names to each list element corresponding to the document and sentence id of each sentence
doc.sent<-paste(SubsetDeepDive[,"docid"],SubsetDeepDive[,"sentid"],sep=".")
names(DeepDivePoses)<-doc.sent

# Extract all the NNPs from DeepDivePoses
# NOTE: Search for CC as to get hits like "Middendorf And Black Creek Formations" which is NNP, CC, NNP, NNP, NNP
DeepDiveNNPs<-sapply(DeepDivePoses,function(x) which(x=="NNP"|x=="CC"))
    
# STEP SEVEN: Find consecutive NNPs in DeepDiveNNPs
print(paste("Find consecutive NNPs in DeepDiveNNPs",Sys.time()))
    
# Consecutive word position locater function:
findConsecutive<-function(DeepDivePoses) {
    Breaks<-c(0,which(diff(DeepDivePoses)!=1),length(DeepDivePoses))
    ConsecutiveList<-lapply(seq(length(Breaks)-1),function(x) DeepDivePoses[(Breaks[x]+1):Breaks[x+1]])
    return(ConsecutiveList)
    }

# Apply function to DeepDiveNNPs list
ConsecutiveNNPs<-sapply(DeepDiveNNPs, function(x) findConsecutive(x))   
# Collapse each cluster into a single character string such that each sentence from formation hits shows its associated clusters    
SentenceNNPs<-sapply(ConsecutiveNNPs,function(y) sapply(y,function(x) paste(x,collapse=",")))
    
# STEP EIGHT: Find words associated with Conescutive NNPs
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
SubsetDeepDiveRow<-sapply(NumClusterVector,function(x) which(SubsetDeepDive[,"docid"]==ClusterData[x,"docid"]&SubsetDeepDive[,"sentid"]==ClusterData[x,"sentid"]))
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
    
# RECORD STATS
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE: 
StepEightDescription<-"Extract NPP clusters from SubsetDeepDive rows"
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE:
StepEightDocs<-length(unique(ClusterData[,"docid"]))
StepEightRows<-length(unique(ClusterData[,"SubsetDeepDiveRow"]))
StepEightClusters<-nrow(ClusterData)
    
# STEP NINE: Extract the rows with clusters with the word 'formation' from ClusterData   
print(paste("Extract 'formation' clusters from ClusterData",Sys.time()))
FormationClusters<-grep(" formation",ClusterData[,"NNPWords"],ignore.case=TRUE,perl=TRUE)
# Extract those rows from ClusterData
FormationData<-ClusterData[FormationClusters,]
FormationData[,"docid"]<-as.character(FormationData[,"docid"])
    
# RECORD STATS
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE: 
StepNineDescription<-"Extract NNP clusters containing the word 'formation'"
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE:
StepNineDocs<-length(unique(FormationData[,"docid"]))
StepNineRows<-length(unique(FormationData[,"SubsetDeepDiveRow"]))
StepNineClusters<-nrow(FormationData)
    
# STEP TEN: Remove Formations that equal to 1 word in length or more than 5 words in length.
print(paste("Remove Formations > 5 or = 1 word(s) in length",Sys.time()))
# Determine the number of words in each NNPWords row
WordLength<-sapply(sapply(FormationData[,"ClusterPosition"], function(x) strsplit(x, ",")), function(x) length(x))
# Determine which rows have more than 5 NNPWords or only 1 NNPWord
BadFormations<-which(WordLength>5|WordLength==1)
# Remove those rows from FormationData
FormationData<-FormationData[-BadFormations,]

# RECORD STATS
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE: 
StepTenDescription<-"Remove Formations > 5 words in length"
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE:
StepTenDocs<-length(unique(FormationData[,"docid"]))
StepTenRows<-length(unique(FormationData[,"SubsetDeepDiveRow"]))
StepTenClusters<-nrow(FormationData)
    
# STEP ELEVEN: Format formation names to have all of the same capitalization patterns.
print(paste("Capitalize formation names appropriately",Sys.time())) 
# First, make all characters in the NNPWords column lower case
FormationData[,"NNPWords"]<-tolower(FormationData[,"NNPWords"])
# Second, capitalize the first letter of each word in the NNPWords column
# Capitalization function:
simpleCap <- function(x) {
  s <- strsplit(x, " ")[[1]]
  paste(toupper(substring(s, 1,1)), substring(s, 2),
      sep="", collapse=" ")
}
    
# Apply simpleCap function to NNPWords column so the first letter of every word is capitalized.
FormationData[,"NNPWords"]<-sapply(FormationData[,"NNPWords"], simpleCap)
    
# STEP TWELVE: Remove all characters after "Formation" or "Formations" in NNPWords column
print(paste(" Remove all characters after 'Formation' or 'Formations'",Sys.time()))
    
# ACCOUNT FOR THE FRENCH EXCEPTIONS WHERE WE WOULD NOT WANT TO REMOVE CHARACTERS AFTER FORMATIONS
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
    
# STEP THIRTEEN: Remove FormationData rows which only have "Formation" in the NNPWords column
print(paste("Capitalize formation names appropriately",Sys.time())) 
FormationData<-FormationData[-which(FormationData[,"NNPWords"]=="Formation"),]
 
# RECORD STATS
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE: 
StepThirteenDescription<-"Remove rows that are just the word 'Formation'"
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE:
StepThirteenDocs<-length(unique(FormationData[,"docid"]))
StepThirteenRows<-length(unique(FormationData[,"SubsetDeepDiveRow"]))
StepThirteenClusters<-nrow(FormationData)        
       
# STEP FOURTEEN: Split the NNPClusters where there is an "And"
SplitFormations<-strsplit(FormationData[,"NNPWords"],'And')
# Remove the blanks created by the splitting
SplitFormationsClean<-sapply(SplitFormations,function(x) unlist(x)[unlist(x)!=""])   
# SplitFormations is a list of the split clusters. Figure out which clusters were split at "And" using length.
SplitCount<-sapply(SplitFormationsClean,length)
# Repeat the data in FormationData for each split cluster by its length
ClusterPosition<-rep(FormationData[,"ClusterPosition"],times=SplitCount) 
docid<-rep(FormationData[,"docid"],times=SplitCount) 
sentid<-rep(FormationData[,"sentid"],times=SplitCount)
# Make a column for the split formations
Formation<-unlist(SplitFormationsClean)
FormationData<-as.data.frame(cbind(Formation,ClusterPosition,docid,sentid))
# Reformat data
FormationData[,"Formation"]<-as.character(FormationData[,"Formation"])
FormationData[,"ClusterPosition"]<-as.character(FormationData[,"ClusterPosition"])
FormationData[,"docid"]<-as.character(FormationData[,"docid"])
FormationData[,"sentid"]<-as.numeric(as.character(FormationData[,"sentid"]))

# Paste "Formation" to the end of the split clusters where it is missing
# Determine the split clusters that DO contain the word "Formation"
FormationHalves<-grep("Formation",FormationData[,"Formation"], perl=TRUE, ignore.case=TRUE)
# Paste "Formation" to all of the non FormationHalves rows
FormationData[-FormationHalves,"Formation"]<-paste(FormationData[-FormationHalves,"Formation"], "Formation", sep=" ")
    
# RECORD STATS
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE: 
StepFourteenDescription<-"Split NNPClusters at 'And'"
# NUMBER OF DOCUMENTS AND ROWS IN SUBSETDEEPDIVE:
StepFourteenDocs<-length(unique(FormationData[,"docid"]))
StepFourteenRows<-dim(unique(FormationData[,c("docid","sentid")]))[1]
StepFourteenClusters<-nrow(FormationData)
    
# STEP FIFTEEN: Remove FormationData rows which only have "Formation" in the NNPWords column
print(paste("Writing Outputs",Sys.time()))
     
# Extract columns of interest for the output
FormationData<-FormationData[,c("Formation","ClusterPosition","docid","sentid")]
   
# Return stats table 
StepDescription<-c(StepOneDescription, StepFourDescription, StepEightDescription, StepNineDescription, StepTenDescription, StepThirteenDescription, StepFourteenDescription)
NumberDocuments<-c(StepOneDocs, StepFourDocs, StepEightDocs, StepNineDocs, StepTenDocs, StepThirteenDocs, StepFourteenDocs)
NumberRows<-c(StepOneRows, StepFourRows, StepEightRows, StepNineRows, StepTenRows, StepThirteenRows, StepFourteenRows)
NumberClusters<-c(StepOneClusters, StepFourClusters, StepEightClusters, StepNineClusters, StepTenClusters, , StepFourteenClusters) 
# Bind Stats Columns
Stats<-cbind(StepDescription,NumberDocuments,NumberRows,NumberClusters)    

# Set directory for output
CurrentDirectory<-getwd()
setwd(paste(CurrentDirectory,"/output",sep=""))
    
# Clear any old output files
unlink("*")

# Write output files
saveRDS(FormationData, "FormationData.rds")
write.csv(FormationData, "FormationData.csv")
write.csv(Stats, "Stats.csv")
    
# Stop the cluster
stopCluster(Cluster)

# COMPLETE
print(paste("Complete",Sys.time())) 
