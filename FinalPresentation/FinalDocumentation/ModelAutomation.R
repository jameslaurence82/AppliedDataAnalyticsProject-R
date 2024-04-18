library(RODBC)
library(caret)
library(randomForest)
library(doParallel)
library(foreach)

# Script Progress is displayed in the CMD window during scheduled task
print("Libraries loaded.")

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

# query Test_RMSE Table from SQL Server to get best RMSE Value
queryRMSE <- "SELECT TOP 1 MODEL_RMSE
              FROM [dbo].[MODEL_RMSE]
              ORDER BY MODEL_TimeStamp DESC" # sorted with newest date


# assign the queryData statement from SQL and assign to R dataframe for modeling
SQLTraining <- sqlQuery(dbconnection, queryTrainingData)

# assign the queryData statement from SQL and assign to R dataframe for modeling
SQLTesting <- sqlQuery(dbconnection, queryTestData)

# assign the queryRMSE statement from SQL and extract Test_RMSE Value and best_RMSE variable
best_RMSE <- sqlQuery(dbconnection,queryRMSE)$MODEL_RMSE[1]

# close sql server connection
odbcClose(dbconnection)

#Remove unneed values and variables
rm(connStr)
rm(queryTrainingData)
rm(queryTestData)
rm(queryRMSE)
rm(dbconnection)

# Script Progress is displayed in the CMD window during scheduled task
print("Connected to SQL Server and fetched data.")
print(paste("Best RMSE Value:", best_RMSE))

######################################################################################
# Prep TRAINING DATA for Machine Learning
######################################################################################

# copy SQLTraining to training_data for ML steps and remove isTestSet column
training_data <- SQLTraining[1:ncol(SQLTraining)-1]

# change date column to be number date (UNIX Epoch)
training_data$FK_DT_Date <- as.numeric(as.POSIXct(training_data$FK_DT_Date))

# remove NA's which will also remove na' from lagged y predictor THA_NextDay_Close column
training_data <- na.omit(training_data)

# Reset row names
rownames(training_data) <- NULL

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
rm(SQLTraining)
rm(SQLTesting)

# Script Progress is displayed in the CMD window during scheduled task
print("Data preparation complete.")

######################################################################################
# Increase Core Use for caret library
######################################################################################

# Register the parallel backend
registerDoParallel(cores=10)

# Script Progress is displayed in the CMD window during scheduled task
print("Parallel processing setup complete.")

######################################################################################
# Random Forest Model Training using Feature Importance view vw_IMPORTANCE_RF 
######################################################################################

# as training data increases, set the repeats to be 10 instead of 10
fitControl <- trainControl(method = "repeatedcv",
                           number = 10,     # number of folds
                           repeats = 10)

modelRF.cv <- train(THA_NextDay_Close ~., data = training_data,
                     method = "rf",
                     trControl = fitControl,
                     metric = "RMSE",
                     ntree=3000)

print(modelRF.cv)

# Script Progress is displayed in the CMD window during scheduled task
print("Model training complete.")

######################################################################################
# compare unseen Test data Random Forest
######################################################################################

# Create a new testing dataset with only the important features
test_x <- testing_data

# Define test_y
test_y <- testing_data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(modelRF.cv, test_x)

# Test Performance
# RMSE - Root Mean Squared Error has the Performance metric assigned from test data
Test_RMSE <- caret::RMSE(test_y, pred_y) 

print(paste("Test RMSE Value:", Test_RMSE))

# Script Progress is displayed in the CMD window during scheduled task
print("Model evaluation on test data complete.")

######################################################################################
# Stop Additional Core Use
######################################################################################

# Stop parallel processing
stopImplicitCluster()

# Script Progress is displayed in the CMD window during scheduled task
print("Stop Parallel Core Usage.")

######################################################################################
# Compare Previous Model Test RMSE from SQL against new Model Test RMSE
# if better -> Save model and export RMSE value to SQL
# if worse -> do nothing
######################################################################################

# Compare model's RMSE with the best RMSE obtained previously
if (Test_RMSE < best_RMSE) { 
  
  #Save Model to binary file
  saveRDS(modelRF.cv, "E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 4\\rfModel.rds")
  
  # Define the file path
  file_path <- "E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 4\\modelrf_details.txt"
  
  # Start fresh: Generate and write a timestamp, overwriting any existing file
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  write(paste("Model run at:", timestamp), file = file_path)  # This overwrites the existing file
  
  # Continue with appending data: Append the model summary
  write("\n\nModel Summary:", file = file_path, append = TRUE)
  capture.output(summary(modelRF.cv), file = file_path, append = TRUE)
  
  # Append performance metrics with headers, ensuring readability
  write("\n\nPerformance Metrics:", file = file_path, append = TRUE)
  write(paste("Previous Test RMSE on unseen data:", best_RMSE), file = file_path, append = TRUE)
  write(paste("New Test RMSE on unseen data:", Test_RMSE), file = file_path, append = TRUE)
  write(paste("MAE:", mean(abs(test_y - pred_y))), file = file_path, append = TRUE)
  residuals <- test_y - pred_y
  ss_res <- sum(residuals^2)
  ss_tot <- sum((test_y - mean(test_y))^2)
  r_squared <- 1 - (ss_res / ss_tot)
  write(paste("R-squared:", r_squared), file = file_path, append = TRUE)
  
  # Note about the training process and adjustments
  write("\n\nTraining Control Update:", file = file_path, append = TRUE)
  write("As training data increases, repeats set to 10 for cross-validation for current data size.", file = file_path, append = TRUE)
  
  # Sql server connection string
  connStr <- "Driver=SQL Server;Server=MSI;Database=ThalesStockPredictor;trusted_connection=yes"
  
  # Reconnect to the SQL Server
  dbconnection <- odbcDriverConnect(connStr)
  
  # Construct the SQL query to insert the RMSE value into the Test_RMSE table
  queryInsert <- paste("INSERT INTO [dbo].[MODEL_RMSE] (MODEL_RMSE) VALUES ( ", Test_RMSE, ")")
  
  # Execute the query to update the RMSE value
  sqlQuery(dbconnection, queryInsert)
  
  # Close the database connection
  odbcClose(dbconnection)
  
  rm(queryInsert)
  rm(connStr)
  rm(dbconnection)
  
} else {
  # Define the file path
  file_path <- "E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 4\\modelrf_details.txt"
  
  # Generate the timestamp message for high RMSE, including the modelRMSE value in the message
  timestamp_msg <- paste("Model run and not saved (High RMSE", modelRMSE, ") at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")
  
  # Read the current content of the file
  if (file.exists(file_path)) {
    current_content <- readLines(file_path)
  } else {
    current_content <- character(0)  # If the file doesn't exist, prepare an empty character vector
  }
  
  # Prepend the new message to the current content
  updated_content <- c(timestamp_msg, current_content)
  
  # Write the updated content back to the file, overwriting the old content
  writeLines(updated_content, file_path)
}

# Script Progress is displayed in the CMD window during scheduled task
print("Model comparison and conditional actions completed.")