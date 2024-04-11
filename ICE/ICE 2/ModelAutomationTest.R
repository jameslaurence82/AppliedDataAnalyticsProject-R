library(RODBC)
library(caret)
library(randomForest)
library(doParallel)
library(foreach)

######################################################################################
# connect to sql server DB thalesstockpredictor to import vw_COMBINED_MODEL view and bestRMSE
######################################################################################

# Sql server connection string
connStr <- "Driver=SQL Server;Server=MSI;Database=ThalesStockPredictor;trusted_connection=yes"

# establish connection to SQL Server
dbconnection <- odbcDriverConnect(connStr)

# query view vw_IMPORTANCE_RF (Random Forest Feature Importance View) from sql server
queryData <- "SELECT * 
              FROM vw_IMPORTANCE_RF
              ORDER BY FK_DT_Date desc" # sorted with newest date

# query MODEL_RSME Table from SQL Server to get best RMSE Value
queryRMSE <- "SELECT TOP 1 MODEL_RMSE
              FROM [dbo].[MODEL_RMSE]
              ORDER BY MODEL_TimeStamp DESC" # sorted with newest date
              

# assign the queryData statement from SQL and assign to R dataframe for modeling
Model_Data <- sqlQuery(dbconnection, queryData)

# assign the queryRSME statement from SQL and extract MODEL_RSME Value and bestRSME variable
bestRSME <- sqlQuery(dbconnection,queryRMSE)$MODEL_RMSE[1]

# close sql server connection
odbcClose(dbconnection)

# remove SQL variables
rm(connStr)
rm(queryData)
rm(queryRMSE)
rm(dbconnection)

######################################################################################
# Prep Model_Data DF for splitting Train/Test/Validate, normalization, correlation
######################################################################################

# copy Model_Data to Model_Norm for ML steps
Model_Norm <- Model_Data

# change date column to be number date (UNIX Epoch)
Model_Norm$FK_DT_Date <- as.numeric(as.POSIXct(Model_Norm$FK_DT_Date))

# remove NA's which will also remove na' from lagged y predictor THA_NextDay_Close column
Model_Norm <- na.omit(Model_Norm)

# Reset row names
rownames(Model_Norm) <- NULL

rm(Model_Data)

######################################################################################
# Split dataframe into Training, Validation, Testing before normalization
######################################################################################

# Ensure reproducibility
set.seed(123)

# Proportion for training set
train_prop <- 0.9

# Split index for training set
train_index <- createDataPartition(y = Model_Norm$THA_NextDay_Close, times = 1, p = train_prop, list = FALSE)

# Create training set
training_data <- Model_Norm[train_index,]

# Create initial test set (which will be split into validation and test sets)
testing_data <- Model_Norm[-train_index,]

# remove data split variables
rm(train_index)
rm(train_prop)
rm(Model_Norm)

######################################################################################
# Increase Core Use for caret library
######################################################################################

# Register the parallel backend
registerDoParallel(cores=11)

######################################################################################
# Random Forest Model Training using Feature Importance view vw_IMPORTANCE_RF 
######################################################################################

fitControl <- trainControl(method = "repeatedcv", 
                           number = 10,     # number of folds
                           repeats = 10)

modelRF.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,],
                    method = "rf", 
                    trControl = fitControl,
                    metric = "RMSE",
                    ntree=2000)

modelRF.cv
# mtry  RMSE      Rsquared   MAE      
# 2    1.075651  0.9989271  0.6922139
# 31    1.098373  0.9988758  0.6982225
# 61    1.107803  0.9988568  0.7067114

######################################################################################
# compare unseen Test data Random Forest
######################################################################################

# Create a new testing dataset with only the important features
test_x <- testing_data

# Define test_y
test_y <- testing_data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(modelRF1.cv, test_x)

# Test Performance
# Performance metrics on the test data
modelRMSE <- caret::RMSE(test_y, pred_y) # RMSE - Root Mean Squared Error

#display RMSE
modelRMSE

# TEST RSME
# [1] 1.022633

pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
#        test_y    pred_y
# 4866  24.30000  24.92498
# 4881  26.85000  26.36565
# 4892  27.68000  27.04225
# 4905  26.87000  27.38446
# 4920  27.05000  26.49179
# 4921  25.72000  25.59194

######################################################################################
# Random Forest Model1 Training using Feature Importance view vw_IMPORTANCE_RF 
######################################################################################

modelRF1.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,],
                     method = "rf", 
                     trControl = fitControl,
                     metric = "RMSE",
                     ntree=2500)

# modelRF1.cv
# mtry  RMSE      Rsquared   MAE      
# 2    1.076762  0.9989239  0.6924424
# 31    1.099059  0.9988738  0.6981158
# 61    1.109110  0.9988531  0.7066556

######################################################################################
# compare unseen Test data Random Forest
######################################################################################

# Create a new testing dataset with only the important features
test_x <- testing_data

# Define test_y
test_y <- testing_data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(modelRF1.cv, test_x)

# Test Performance
# Performance metrics on the test data
modelRMSE <- caret::RMSE(test_y, pred_y) # RMSE - Root Mean Squared Error

#display RMSE
modelRMSE

# TEST RSME
# [1] 1.021826

pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
#        test_y    pred_y
# 4866  24.30000  24.96616
# 4881  26.85000  26.37287
# 4892  27.68000  27.04567
# 4905  26.87000  27.37848
# 4920  27.05000  26.53954
# 4921  25.72000  25.56958

######################################################################################
# Random Forest Model2 Training using Feature Importance view vw_IMPORTANCE_RF 
######################################################################################

modelRF2.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,],
                     method = "rf", 
                     trControl = fitControl,
                     metric = "RMSE",
                     ntree=3000)

modelRF2.cv

# mtry  RMSE      Rsquared   MAE      
# 2    1.075038  0.9989284  0.6920066
# 31    1.098144  0.9988763  0.6981018
# 61    1.107796  0.9988567  0.7066651

######################################################################################
# compare unseen Test data Random Forest
######################################################################################

# Create a new testing dataset with only the important features
test_x <- testing_data

# Define test_y
test_y <- testing_data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(modelRF2.cv, test_x)

# Test Performance
# Performance metrics on the test data
modelRMSE <- caret::RMSE(test_y, pred_y) # RMSE - Root Mean Squared Error

#display RMSE
modelRMSE

# TEST RSME
# [1] 1.020942

pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
#        test_y    pred_y
# 4866  24.30000  24.90944
# 4881  26.85000  26.37207
# 4892  27.68000  27.04545
# 4905  26.87000  27.38002
# 4920  27.05000  26.50417
# 4921  25.72000  25.57562

######################################################################################
# Stop Additional Core Use
######################################################################################

# Stop parallel processing
stopImplicitCluster()

######################################################################################
# Compare Previous Model RMSE from SQL against new Model RMSE
# if better -> Save model and export RMSE value to SQL
# if worse -> do nothing
######################################################################################

# Compare model's RMSE with the best RMSE obtained previously
if (modelRMSE < bestRSME) { 
  
  #Save Modelto binary file
  saveRDS(modelRF2.cv, "E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 2\\rfModel.rds")
  
  # Sql server connection string
  connStr <- "Driver=SQL Server;Server=MSI;Database=ThalesStockPredictor;trusted_connection=yes"
  
  # Reconnect to the SQL Server
  dbconnection <- odbcDriverConnect(connStr)
  
  # Construct the SQL query to insert the RMSE value into the MODEL_RSME table
  queryInsert <- paste("INSERT INTO [dbo].[MODEL_RMSE] (MODEL_RMSE) VALUES ( ", modelRMSE, ")")
    
  # Execute the query to update the RMSE value
  sqlQuery(dbconnection, queryInsert)
  
  # Close the database connection
  odbcClose(dbconnection)
  
  rm(queryInsert)
  rm(connStr)
  rm(dbconnection)
}

