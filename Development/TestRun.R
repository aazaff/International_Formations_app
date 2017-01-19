library("RCurl")
library("RJSONIO")
library("stringdist")
library("doParallel")

# download all references from PBDB
RefsURL<-"https://paleobiodb.org/data1.2/taxa/refs.csv?select=taxonomy&private&all_records"
GotURL<-getURL(RefsURL)
PBDBRefs<-read.csv(text=GotURL,header=TRUE)

# download all article data form geodeepdive with a pubname that contains the word "palaeogeography"
DDRefs<-fromJSON("https://geodeepdive.org/api/articles?pubname_like=palaeogeography")
DDRefs<-DDRefs[[1]][[2]]

# make a column of DD reference numbers
DDRefNum<-sapply(DDRefs,function(x) x[["id"]])
# make a vector of DD authors
DDAuthors<-sapply(DDRefs,function(x) paste(unlist(x[["author"]]),collapse=" "))
# make a vector of DD publication years
DDPubYr<-sapply(DDRefs,function(x) x[["year"]])
# make a vector of DD ref titles 
DDTitles<-sapply(DDRefs,function(x) x[["title"]])
# make a column of DD jornalnames 
DDJournals<-sapply(DDRefs,function(x) x[["journal"]])
# create identically formatted matrices for geodeepdive and pbdb references 
DDRefs<-cbind(DDRefNum,DDAuthors,DDPubYr,DDTitles,DDJournals)
PBDBRefs<-cbind(PBDBRefs[c("reference_no","author1last","pubyr","reftitle","pubtitle")])

# convert matrices to dataframes
DDRefs<-as.data.frame(DDRefs)
PBDBRefs<-as.data.frame(PBDBRefs)

# Reformat DDRefs to match PBDBRefs
DDRefs[,"DDRefNum"]<-as.character(DDRefs[,"DDRefNum"])
DDRefs[,"DDAuthors"]<-as.character(DDRefs[,"DDAuthors"])
DDRefs[,"DDPubYr"]<-as.numeric(as.character(DDRefs[,"DDPubYr"]))
DDRefs[,"DDTitles"]<-as.character(DDRefs[,"DDTitles"])
DDRefs[,"DDJournals"]<-as.character(DDRefs[,"DDJournals"])
DDRefs[,"author"]<-DDRefs[,"DDAuthors"]

# assign DDRefs columnames
colnames(DDRefs)<-c("reference_no","author","pubyr","title","pubtitle")
 
# Reformat the PBDBRefs to match DDRefs
PBDBRefs[,"reference_no"]<-as.numeric(as.character(PBDBRefs[,"reference_no"]))
PBDBRefs[,"author1last"]<-as.character(PBDBRefs[,"author1last"])
PBDBRefs[,"pubyr"]<-as.numeric(as.character(PBDBRefs[,"pubyr"]))
PBDBRefs[,"reftitle"]<-as.character(PBDBRefs[,"reftitle"])
PBDBRefs[,"pubtitle"]<-as.character(PBDBRefs[,"pubtitle"])

 # Assign column names
colnames(PBDBRefs)<-c("reference_no","author","pubyr","title","pubtitle")

# subset PBDBRefs to only 100 references
PBDBRefs<-PBDBRefs[1:100,]

### Phase 2: A MATCHING FUNCTION IS BORN
    
print(paste("perform title matches",Sys.time()))   

# Find the stringsim for titles
Title<-sapply(PBDBRefs[,"pbdb_title"],function(x,y) which.max(stringsim(x,y)),DDRefs[,"gdd_title"])
 
# A function for matching PBDB and DDRefs
matchTitles<-function(Bib1,Bib2) {
    # Title Similarity
    Title<-stringsim(Bib1["title"],Bib2["title"])
    DDRefNum<-as.character(Bib1["reference_no"])
    PBDBRefNum<-as.numeric(Bib2["reference_no"])
    # Return output     
    return(setNames(c(PBDBRefNum,DDRefNum,Title),c("PBDBNum","DDRefNum","Title")))
    }

# A macro function for matching PBDB and DDRefs
macroTitles<-function(PBDBRefs,DDRefs) {    
    TemporaryMatches<-as.data.frame(t(apply(DDRefs,1,matchTitles,PBDBRefs)))
    return(TemporaryMatches[which.max(TemporaryMatches[,"Title"]),])
    }
    
# Establish a cluster for doParallel
# Make Core Cluster 
Cluster<-makeCluster(3)
# Pass the functions to the cluster
clusterExport(cl=Cluster,varlist=c("matchTitles","stringsim","macroTitles"))
MatchTitles<-parApply(Cluster, PBDBRefs, 1, macroTitles, DDRefs)
# Stop the cluster
stopCluster(Cluster)

# Convert PBDBReferences into a data frame
MatchTitles<-do.call(rbind,MatchTitles)

# Merge all three data frame
DDMatchedRefs<-merge(DDRefs,MatchTitles,by.x="reference_no",by.y="DDRefNum",all.y=TRUE)
# Reorder the DDMatchedRefs so it matched PBDBRefs
DDMatchedRefs<-DDMatchedRefs[order(DDMatchedRefs[,"PBDBRefNum"]),]

# A function for matching PBDB and DDMatchedRefs
matchBibs<-function(FinalRefs) {
    # Pub year match
    Year<-FinalRefs["pbdb_year"]==FinalRefs["gdd_year"]
    # Journal Similarity
    Journal<-stringsim(FinalRefs["pbdb_pubtitle"],Finalrefs["gdd_pubtitle"])
    # Author present
    Author<-grepl(FinalRefs["author"],Bib1["author"],perl=TRUE,ignore.case=TRUE)
    # Add docid column 
    DDRefNum<-as.character(Bib1["reference_no"])
    PBDBRefNum<-as.numeric(Bib2["reference_no"])
    # Return output     
    return(setNames(c(PBDBRefNum,DDRefNum,Year,Journal,Author),c("PBDBRefNum","DDRefNum","Year","Journal","Author")))
    }

# A macro function for matching PBDB and DDTitlesMatches
#macroBibs<-function(PBDBRefs,DDMatchedRefs) {    
    #MatchReferences<-as.data.frame(t(apply(DDMatchedRefs,1,matchBibs,PBDBRefs)))
    #return(MatchReferences)
    #}
    
# Establish a cluster for doParallel
# Make Core Cluster 
Cluster<-makeCluster(3)
# Pass the functions to the cluster
clusterExport(cl=Cluster,varlist=c("matchBibs","stringsim","macroBibs"))
MatchReferences<-parApply(Cluster, DDMatchedRefs, 1, matchBibs, PBDBRefs)
stopCluster(Cluster)
