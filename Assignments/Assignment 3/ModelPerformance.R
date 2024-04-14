library(RODBC)
library(caret)
library(randomForest)

######################################################################################
# connect to sql server DB thalesstockpredictor to import vw_IMPORTANCE_RF view and bestRMSE
######################################################################################

# Sql server connection string
connStr <- "Driver=SQL Server;Server=MSI;Database=ThalesStockPredictor;trusted_connection=yes"

# establish connection to SQL Server
dbconnection <- odbcDriverConnect(connStr)

# query view vw_IMPORTANCE_RF (Random Forest Feature Importance View) from sql server
queryTrainingData <- "SELECT * 
              FROM vw_TRAINING_DATA
              ORDER BY FK_DT_Date desc" # sorted with newest date

# query view vw_IMPORTANCE_RF (Random Forest Feature Importance View) from sql server
queryTestData <- "SELECT * 
              FROM vw_TESTING_DATA
              ORDER BY FK_DT_Date desc" # sorted with newest date

# assign the queryData statement from SQL and assign to R dataframe for modeling
SQLTesting <- sqlQuery(dbconnection, queryTestData)

# close sql server connection
odbcClose(dbconnection)

#Remove unneed values and variables
rm(connStr)
rm(queryTestData)
rm(queryRMSE)
rm(dbconnection)

######################################################################################
# Prep TESTING DATA for splitting and Machine Learning
######################################################################################

# copy SQLTesting to testing_data for ML steps and remove isTestSet column
testing_data <- SQLTesting[1:ncol(SQLTesting)-1]

# change date column to be number date (UNIX Epoch)
testing_data$FK_DT_Date <- as.numeric(as.POSIXct(testing_data$FK_DT_Date))

# remove NA's which will also remove na' from lagged y predictor THA_NextDay_Close column
testing_data <- na.omit(testing_data)

# Reset row names
rownames(testing_data) <- NULL

#Remove unneed values and variables
rm(SQLTesting)

######################################################################################
# Load and test Model on unseen Data
######################################################################################

# Load Model
rfmodel <- readRDS("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 4\\rfModel.rds")

# Pull best Trained Model Results
bestModel <- rfmodel$results[which.min(rfmodel$results$RMSE),]

# Select columns, excluding the SD columns if they exist
required_columns <- c("mtry", "RMSE", "Rsquared", "MAE")  # Add more columns as necessary
bestModel<- bestModel[, required_columns, drop = FALSE]

# Create testing dataset
test_x <- testing_data

# Define test_y
test_y <- testing_data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(rfmodel, test_x)

# RMSE - Root Mean Squared Error has the Performance metric assigned from test data
Test_RMSE <- caret::RMSE(test_y, pred_y) 

# Calculated Performance Outputs
MAE <- mean(abs(test_y - pred_y))
residuals <- test_y - pred_y
ss_res <- sum(residuals^2)
ss_tot <- sum((test_y - mean(test_y))^2)
r_squared <- 1 - (ss_res / ss_tot)

# Create a dataframe with these metrics
metrics_df <- data.frame(
  Test_RMSE = Test_RMSE,
  Rsquared = r_squared,
  MAE = MAE
)
