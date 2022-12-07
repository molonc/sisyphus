# Below is the code to install the needed packages and set your working directory (wd)

# set the wd to where you have the metrics files and chasmbot_brain.R - make sure to use forward slashes
setwd("~/sisyphus/workflows/chasmbot")

suppressMessages(library(ggplot2)) # for graphing
suppressMessages(library(tidyr)) # for data manipulation
suppressMessages(library(glmnet)) # for machine learning
suppressMessages(library(stringr)) # for string manipulation
suppressMessages(library(dplyr)) # for table manipulation
suppressMessages(library(tibble)) # for table editing
source("chasmbot_brain.R")

# INSTRUCTIONS
# 1) Go to Azure Storage Explorer > singlecellresults BLOB > type the analysis jiRA ticket ID
# 2) Go to results/annotation/ and find the _metrics.csv.gz file, DOWNLOAD IT, and put it into folder listed above
# 3) Press "Source" in the top right

files <- list.files(pattern = "metrics.csv.gz")
for (file in files) {
  cat("****************************", sep = "\n")
  cat(file, sep = "\n")
  cat("****************************", sep = "\n")
  chasmbrain(file)
}


