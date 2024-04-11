library(RODBC)
library(caret)
library(randomForest)
library(doParallel)
library(foreach)

######################################################################################
# connect to sql server DB thalesstockpredictor to export vw_COMBINED_MODEL view
######################################################################################

# Sql server connection string
connStr <- "Driver=SQL Server;Server=MSI;Database=ThalesStockPredictor;trusted_connection=yes"

# establish connection
dbconnection <- odbcDriverConnect(connStr)
#
# # query the COMBINED_MODEL View that has all attributes from sql server
# # this is all the combined tables with y value column
# query1 <- "SELECT * FROM vw_COMBINED_MODEL
#           ORDER BY FK_DT_Date desc" # sorted with newest date

# query Random Forest FEATURE IMPORTANCE MODEL vw_IMPORTANCE_RF view from sql server
query1 <- "SELECT * FROM vw_IMPORTANCE_RF
          ORDER BY FK_DT_Date desc" # sorted with newest date

# assign the query to r dataframes for modeling
Model_Data <- sqlQuery(dbconnection, query1)
#
# close sql server connection
odbcClose(dbconnection)

# remove SQL variables
rm(connStr)
rm(query1)
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

# rm(Model_Data)
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

######################################################################################
# Increase Core Use for caret library
######################################################################################

# Register the parallel backend
registerDoParallel(cores=10)

######################################################################################
# Feature Importance - Random Forest
######################################################################################

# modelRF_importance <- randomForest(THA_NextDay_Close ~ ., data = training_data[1:5186,], importance = TRUE, trControl = fitControl)
# 
# # Get feature importance
# importance <- importance(modelRF_importance)
# 
# data <- data.frame(varImp(modelRF_importance, scale = TRUE))
# 
# importance_sort <- sort()
# 
# # Sort the importance in descending order
# rf_importance_sort <- sort(rf_importance, decreasing = TRUE)
# 
# # Get the names of the sorted features
# feature_names <- rownames(importance_with_indices)[importance_sorted$ix]
# 
# # Combine the names and importance into a data frame
# importance_df <- data.frame(Feature = feature_names, Importance = importance_sorted$x)
# 
# # Print the data frame
# options(scipen = 999)  # This will disable scientific notation
# print(importance_df)

# Filter the features with importance above 60
# important_features <- importance_df$Feature[importance_df$Importance > 60]

######################################################################################
# Random Forest Model Training using Feature Importance view vw_IMPORTANCE_RF 
######################################################################################

fitControl <- trainControl(method = "repeatedcv", 
                           number = 10,     # number of folds
                           repeats = 10)

modelRF.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,], method = "rf", trControl = fitControl)

modelRF.cv
# mtry  RMSE      Rsquared   MAE      
# 2    1.078374  0.9989217  0.6947723
# 31    1.099065  0.9988744  0.6987653
# 61    1.108354  0.9988555  0.7071473

fitControl1 <- trainControl(method = "repeatedcv", 
                           number = 15,     # number of folds
                           repeats = 15)

modelRF1.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,], method = "rf", trControl = fitControl)

rm(modelRF1.cv)

# mtry  RMSE      Rsquared   MAE      
# 2    1.080848  0.9989169  0.6954043
# 31    1.097844  0.9988764  0.6977139
# 61    1.106552  0.9988588  0.7055460

fitControl2 <- trainControl(method = "repeatedcv", 
                            number = 15,     # number of folds
                            repeats = 10)

modelRF2.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,], method = "rf", trControl = fitControl)
rm(modelRF2.cv)
modelRF2.cv
# mtry  RMSE      Rsquared   MAE      
# 2    1.077814  0.9989220  0.6933179
# 31    1.098482  0.9988741  0.6972192
# 61    1.107597  0.9988555  0.7054244

fitControl <- trainControl(method = "repeatedcv", 
                           number = 10,     # number of folds
                           repeats = 10)
rm(tuneGrid)
tuneGrid <- expand.grid(.mtry = c(10, 20, 30)) 

modelRF.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,], method = "rf", trControl = fitControl, tuneGrid = tuneGrid, metric = "RMSE", ntree=500)

modelRF.cv
# 
# mtry  RMSE      Rsquared   MAE      
# 10    1.070452  0.9989274  0.6784740
# 20    1.090147  0.9988879  0.6920435
# 30    1.097247  0.9988739  0.6978337

modelRF3.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,], 
                    method = "rf", trControl = fitControl, 
                    metric = "RMSE", ntree=1000)

modelRF3.cv

# mtry  RMSE      Rsquared   MAE      
# 2    1.075296  0.9989255  0.6928574
# 31    1.097846  0.9988734  0.6986351
# 61    1.107162  0.9988546  0.7072652

modelRF4.cv <- train(THA_NextDay_Close ~., data = training_data[1:5186,], 
                     method = "rf", trControl = fitControl, 
                     metric = "RMSE", ntree=2000)

modelRF4.cv
# mtry  RMSE      Rsquared   MAE      
# 2    1.074143  0.9989306  0.6921641
# 31    1.099173  0.9988747  0.6976261
# 61    1.108573  0.9988557  0.7055408

######################################################################################
# compare unseen Test data Random Forest
######################################################################################

##### RANDOM FOREST MODEL 3 ####
# Create a new testing dataset with only the important features
test_x <- testing_data

# Define test_y
test_y <- testing_data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(modelRF.cv, test_x)

# Test Performance
# Performance metrics on the test data
caret::RMSE(test_y, pred_y) # RMSE - Root Mean Squared Error

# TEST RSME
# [1] 1.071261
##############

pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
#        test_y    pred_y

##### RANDOM FOREST MODEL 3 ####
# Create a new testing dataset with only the important features
test_x <- testing_data

# Define test_y
test_y <- testing_data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(modelRF3.cv, test_x)

# Test Performance
# Performance metrics on the test data
caret::RMSE(test_y, pred_y) # RMSE - Root Mean Squared Error


# TEST RSME
# [1] 1.025251
##############

pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
#        test_y    pred_y
# 4866  24.30000  24.92666
# 4881  26.85000  26.37806
# 4892  27.68000  27.06222
# 4905  26.87000  27.40158
# 4920  27.05000  26.49570

##### RANDOM FOREST MODEL 4 ####
# Create a new testing dataset with only the important features
test_x <- testing_data

# Define test_y
test_y <- testing_data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(modelRF4.cv, test_x)

# Test Performance
# Performance metrics on the test data
caret::RMSE(test_y, pred_y) # RMSE - Root Mean Squared Error


# TEST RSME
# [1] 1.023909
##############

pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
#        test_y    pred_y
# 4866  24.30000  24.96480
# 4881  26.85000  26.38047
# 4892  27.68000  27.03916
# 4905  26.87000  27.37856
# 4920  27.05000  26.52013

######################################################################################
# Stop Additional Core Use
######################################################################################

# Stop parallel processing
stopImplicitCluster()

######################################################################################
# Save the best model --> RandomForest Using Feature Importance
######################################################################################

saveRDS(modelRF4.cv, "E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 2\\rfModel.rds")

######################################################################################
# loading Saved Model for Predictions --> RandomForest Using Feature Importance
######################################################################################

savedRF_Model <- readRDS("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 2\\rfModel.rds")

#use model to make predictions on test data
pred_y = predict(savedRF_Model, test_x)

#Test Performance from Saved Model File
# performance metrics on the test data
caret::RMSE(test_y, pred_y) #rmse - Root Mean Squared Error

# RSME
# [1] 1.023909

# Predictions from Saved Model File
pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
#        test_y    pred_y
# 4866  24.30000  24.96480
# 4881  26.85000  26.38047
# 4892  27.68000  27.03916
# 4905  26.87000  27.37856
# 4920  27.05000  26.52013
# 4921  25.72000  25.58692

######################################################################################
# Extract the optimal parameters and RMSE
######################################################################################
# 
# optimal_mtry <- modelRF.cv$bestTune$mtry
# optimal_ntree <- modelRF.cv$bestTune$ntree
# optimal_rmse <- min(modelRF.cv$results$RMSE)
# 
# # Create a data frame
# df <- data.frame(mtry = optimal_mtry, ntree = optimal_ntree, RMSE = optimal_rmse)
# 
# # Write the data frame to a SQL table
# dbWriteTable(con, "model_performance", df, append = TRUE)