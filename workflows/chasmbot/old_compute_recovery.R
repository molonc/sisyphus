# THIS IS TO TEACH THE ROBOT THE SKILLSET

# Makes it go you Desktop folder :D
setwd("C:/Users/jbwang/Desktop/chasmbot")

options(stringsAsFactors = FALSE)
.libPaths(c("C:/Users/jbwang/Desktop/R-packages", "C:/Program Files/R/R-3.6.1/library"))

library(ggplot2) # for graphing
library(tidyr) # for data manipulation
library(glmnet) # for machine learning
library(stringr) # for string manipulation
library(dplyr) # for table manipulation
library(tibble) # for table editing
source("chasmbot_brain.R")

#############
# START HERE!!
#############

# INSTRUCTIONS
# 1) Go to Azure Storage Explorer > singlecellresults BLOB > type the analysis jiRA ticket ID
# 2) Go to results/annotation/ and find the _metrics.csv.gz file, DOWNLOAD IT, and put it into "chasmbot" folder
# 3) Update the "input" variable below, you can use the "auto-complete trick" to get the file name (ask Justina)
# 4) Press "Source" in the top right

# input <- "A108879B_metrics.csv.gz"
# chasmbrain(input)


# ADVANCED HACKING!!!
# JUST throw things into chasm bot, it'll do everything else :(
files <- list.files(pattern = "_metrics.csv.gz")
for (file in files) {
  cat("****************************", sep = "\n")
  cat(file, sep = "\n")
  cat("****************************", sep = "\n")
  chasmbrain(file)
}

