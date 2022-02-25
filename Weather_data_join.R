rm(list = ls()) #clear workspace
library(dplyr)
library(doParallel)
library(caret)
library(smotefamily)

setwd("H:\\Dropbox\\Dropbox\\Frankfurt Courses\\Data Science\\2021\\Termpaper") #set working directory
#setwd("~/R/dropbox/Frankfurt Courses/Data Science/Termpaper") #set working directory

source("DailyLevelData_analysis_functions.r")

# ----
# This loop extracts the weather data, and add it to the yelp data.
if(0){
  #load data
  yelp_data <- read.csv("DailyLevel_data_rev_Imputed.csv",header=TRUE,skipNul = T) #read csv file
  yelp_data$date <- as.Date(yelp_data$date)
  yelp_data$X=NULL
  
  #---- read the temperature data
  wear=extractweather(yelp_data,resol=.75)
  
  # take the averages across stations for each coordinate
  weather=weardailyavg(wear)
  
  
  
  dates=sort(unique(yelp_data$date))
  weatherstations=as.data.frame(t(sapply(weather,function(x){colMeans(x$range)})))
  
  # adding weather data to yelp_data
  if(1){
    stations_by=t(apply(yelp_data[,c("business_lat","business_long")],1,
                        function(x){a=sort((x[1]-weatherstations$rangelat)^2+
                                             (x[2]-weatherstations$rangelong)^2,index.return=T)
                        return(a$ix[1:50])})) # finding the 50 closest stations
    
    # add for example, temperature forecasts to the weather data
    for(i in 1:length(weather)){
      if(nrow(weather[[i]]$data)==0)
        next
      store_weather=weather[[i]]$data
      store_weather$TOBS_1=c(store_weather$TOBS[2:nrow(store_weather)],NA)
      store_weather$TOBS_2=c(store_weather$TOBS[3:nrow(store_weather)],NA,NA)
      store_weather$TOBS_3=c(store_weather$TOBS[4:nrow(store_weather)],NA,NA,NA)
      store_weather$TOBS_4=c(store_weather$TOBS[5:nrow(store_weather)],NA,NA,NA,NA)
      weather[[i]]$data=store_weather
    }
    weatherinf=colnames(store_weather)[-1] # which weather variables are available?
    
    yelp_data_weather=NULL
    for(i in 1:length(weather)){
      k=1 # start with the closest station
      stores_in=stations_by[,k]==i
      if(sum(stores_in)==0)
        next
      store_weather=weather[[i]]$data
      
      temp=yelp_data[stores_in,]
      temp=merge(temp,store_weather,by.x="date",by.y="DATE",all.x=T)
      yelp_data_weather=rbind(yelp_data_weather,temp)
      print(i)
    }
    
    # now deal with the missings, by going to the next possible station
    temp_indx=is.na(yelp_data_weather[,"TOBS"])|is.na(yelp_data_weather[,"PRCP"])
    k_changed=NULL
    for(i in which(temp_indx)){
      temp_date=yelp_data_weather[i,]$date
      for(k in 2:ncol(stations_by)){
        temp=weather[[stations_by[i,k]]]$data
        if(!is.na(as.numeric(temp[temp$DATE==temp_date,"TOBS"]))&!is.na(as.numeric(temp[temp$DATE==temp_date,"PRCP"])))
          break
      }
      k_changed=c(k_changed,k)
      
      yelp_data_weather[i,weatherinf]=temp[temp$DATE==temp_date,-1]
      #print(i)
    }
    
    # add weekends and quarters
    temp=weekdays(yelp_data_weather$date,abbreviate = T)
    yelp_data_weather$WE=temp=="Sa"|temp=="So"
    
    yelp_data_weather$Quarter=as.factor(quarters(yelp_data_weather$date))
    
    #save(file="yelp_data_weather.RData",list=c("yelp_data_weather"))
    write.csv(yelp_data_weather,file="yelp_data_rev_weather.csv")
    
  }
  
}


# ----
# Importing and adjusting the yelp-data + weather data
yelp_data_weather=read.csv(file="yelp_data_rev_weather.csv")
#load("yelp_data_weather.RData")

# some adjustments to the imported data
yelp_data=yelp_data_weather

yelp_data$date = as.Date(yelp_data$date)
yelp_data$ch_in_string[yelp_data$ch_in>=1]="ch_in"
yelp_data$ch_in_string[yelp_data$ch_in==0]="Noch_in"
yelp_data$ch_in_string <- as.factor(yelp_data$ch_in_string)
yelp_data$ch_in_string <- relevel(yelp_data$ch_in_string,ref="ch_in") # since the performance evaluations are mainly made
# to check for the minority class - in our case Noch_in


yelp_data$business_park=as.factor(yelp_data$business_park)
yelp_data$business_open=as.factor(yelp_data$business_open)
yelp_data$business_cat=as.factor(yelp_data$business_cat)
yelp_data$WE=as.factor(yelp_data$WE)
yelp_data$Quarter=as.factor(yelp_data$Quarter)