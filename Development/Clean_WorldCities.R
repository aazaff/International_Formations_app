# Load world cities data
WorldCities<-read.csv("~/Documents/DeepDive/worldcities2.csv")
CountryCodes<-read.csv("~/Documents/DeepDive/International_Formations/CountryCodes.csv")
#Select two-letter country codes
CountryCodes[,"ISO.CODES"]<-gsub("( ).*","\\1",CountryCodes[,"ISO.CODES"])
CountryCodes[,"ISO.CODES"]<-gsub(" ","",CountryCodes[,"ISO.CODES"]) 
test<-merge(WorldCities,CountryCodes[,c("COUNTRY","POPULATION","AREA.KM2","GDP..USD","ISO.CODES")],by.x="ISO.3166.1.country.code", by.y="ISO.CODES", all.x=TRUE)        
# DEAL WITH BLANKS AND ERRORS CREATED BY "NA" COUNTRY CODE FOR NAMIBIA
Blanks<-which(test[,"ISO.3166.1.country.code"]=="")
test[,"ISO.3166.1.country.code"]<-as.character(test[,"ISO.3166.1.country.code"])
test[Blanks,"ISO.3166.1.country.code"]<-"BLANK" 
test[which(is.na(test[,"ISO.3166.1.country.code"])),"ISO.3166.1.country.code"]<-"NA"
test[which(test[,"ISO.3166.1.country.code"]=="NA"),"COUNTRY"]<-"Namibia"   
test[which(test[,"COUNTRY"]=="Namibia"),"POPULATION"]<-CountryCodes[which(CountryCodes[,"COUNTRY"]=="Namibia"),"POPULATION"]
test[which(test[,"COUNTRY"]=="Namibia"),"AREA.KM2"]<-CountryCodes[which(CountryCodes[,"COUNTRY"]=="Namibia"),"AREA.KM2"]
test[which(test[,"COUNTRY"]=="Namibia"),"GDP..USD"]<-CountryCodes[which(CountryCodes[,"COUNTRY"]=="Namibia"),"GDP..USD"]
test[which(is.na(test[,"GNS.UFI"])&test[,"name"]==""),"ISO.3166.1.country.code"]<-"KM"
test[which(is.na(test[,"GNS.UFI"])&test[,"name"]==""),"FIPS.5.2.subdivision.code"]<-2
test[which(is.na(test[,"GNS.UFI"])&test[,"name"]==""),"GNS.FD"]<-"PPLC"
test[which(is.na(test[,"GNS.UFI"])&test[,"name"]==""),"GNS.UFI"]<-"-2087938"
test[which(test[,"name"]==""),"ISO.639.1.language.code"]<-"ar"
test[which(test[,"name"]==""),"language.script"]<-"latin"
test[which(test[,"name"]==""),"latitude"]<-"-11.704167"
test[which(test[,"name"]==""),"longitude"]<-"43.240278"
test[which(test[,"name"]==""),"POPULATION"]<-CountryCodes[which(CountryCodes[,"ISO.CODES"]=="KM"),"POPULATION"]
test[which(test[,"name"]==""),"AREA.KM2"]<-CountryCodes[which(CountryCodes[,"ISO.CODES"]=="KM"),"AREA.KM2"]
test[which(test[,"name"]==""),"GDP..USD"]<-CountryCodes[which(CountryCodes[,"ISO.CODES"]=="KM"),"GDP..USD"]
test[which(test[,"name"]==""),"COUNTRY"]<-CountryCodes[which(CountryCodes[,"ISO.CODES"]=="KM"),"COUNTRY"]
test[which(test[,"name"]==""),"name"]<-"Moroni"
test[which(test[,"ISO.3166.1.country.code"]=="GF"),"Country"]<-"French Guiana"
test[which(test[,"ISO.3166.1.country.code"]=="MQ"),"Country"]<-"Martinique"
test[which(test[,"ISO.3166.1.country.code"]=="TF"),"Country"]<-"French Southern Territories"
test[,"COUNTRY"]<-as.character(test[,"COUNTRY"])
test[which(test[,"ISO.3166.1.country.code"]=="GF"),"COUNTRY"]<-"French Guiana"
test[which(test[,"ISO.3166.1.country.code"]=="MQ"),"COUNTRY"]<-"Martinique"
test[which(test[,"ISO.3166.1.country.code"]=="TF"),"COUNTRY"]<-"French Southern Territories"
test[which(test[,"ISO.3166.1.country.code"]=="GP"),"COUNTRY"]<-"Guadeloupe"

