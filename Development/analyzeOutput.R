# Custom functions are camelCase. Arrays, parameters, and arguments are PascalCase
# Dependency functions are not embedded in master functions
# []-notation is used wherever possible, and $-notation is avoided.

######################################### Load Required Libraries ###########################################
# Load Required Libraries
library("RPostgreSQL")
library("pbapply")
library("plyr")
library("stringdist")

Driver<-dbDriver("PostgreSQL") # Establish database driver
Panda<-dbConnect(Driver, dbname = "PANDA", host = "localhost", port = 5432, user = "zaffos")

#############################################################################################################
###################################### OUTPUT LOADING FUNCTIONS, PANDA ######################################
#############################################################################################################
# No functions at this time.
 
################################################ OUTPUT: Load Data ##########################################
# Upload the raw data from postgres
MatchReferences<-dbGetQuery(Panda, "SELECT * FROM panda_12052016.match_references;")

# Upload the learning set
LearningSet<-dbGetQuery(Panda, "SELECT * FROM panda_12052016.learning_set;")

# Check the plausible regression models
Model1<-glm(Match~title_sim,family="binomial",data=LearningSet)
Model2<-glm(Match~title_sim+author_in,family="binomial",data=LearningSet)
Model3<-glm(Match~title_sim+author_in+year_match,family="binomial",data=LearningSet)
Model4<-glm(Match~title_sim+author_in+year_match+pubtitle_sim,family="binomial",data=LearningSet)

# Make predictions from the basic set
Probabilities<-round(predict(Model4,MatchReferences,type="response"),4)
