library(RODBC)
library(caret)
library(dplyr)
library(randomForest)
library(xgboost)
library(doParallel)
library(foreach)
library(rpart)
library(glmnet)

######################################################################################
# connect to sql server DB thalesstockpredictor to export vw_COMBINED_MODEL view
######################################################################################

# Sql server connection string
connStr <- "Driver=SQL Server;Server=MSI;Database=ThalesStockPredictor;trusted_connection=yes"

# establish connection
dbconnection <- odbcDriverConnect(connStr)
#
# query each view from sql server
# this is all the combined tables with y value column
query1 <- "SELECT * FROM vw_COMBINED_MODEL
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
initial_test_data <- Model_Norm[-train_index,]

# Create testing set
testing_data <- initial_test_data

# remove data split variables
rm(train_index)
rm(initial_test_data)
rm(train_prop)

######################################################################################
# XGBoost Model
######################################################################################

train_x = data.matrix(training_data[,1:85])
train_y = training_data[,86]

watchlist = list(train=train_x)

Modelbst <- xgb.cv(data = train_x, label = train_y, nfold = 10, watchlist=watchlist,
              nrounds = 1500, nthread = 8, depth= 5)
Modelbst
##### xgb.cv 10-folds
# iter train_rmse_mean train_rmse_std test_rmse_mean test_rmse_std
# <num>           <num>          <num>          <num>         <num>
# 1    46.042402030   0.1459651482      46.025132    1.33403246
# 2    32.314164558   0.1020708105      32.317680    0.95489832
# 3    22.685298897   0.0710987979      22.683929    0.67258156
# 4    15.935785613   0.0499139269      15.935509    0.47471401
# 5    11.208180356   0.0349238633      11.210675    0.34262934
# ---                                                                  
# 1496     0.001058564   0.0000293215       1.247539    0.08832847
# 1497     0.001058564   0.0000293215       1.247539    0.08832847
# 1498     0.001058564   0.0000293215       1.247539    0.08832847
# 1499     0.001058564   0.0000293215       1.247539    0.08832847
# 1500     0.001058564   0.0000293215       1.247539    0.08832847

# hyper Paramaters
params <- list(
  objective = "reg:squarederror",  # Regression task
  eta = 0.1,                      # Learning rate (step size)
  gamma = 0,                      # Regularization parameter
  colsample_bytree = 0.8,         # Fraction of features to consider
  min_child_weight = 1,           # Minimum sum of instance weight
  subsample = 0.8,                # Fraction of samples to consider
  max_depth = 3
)
Modelbst <- xgb.cv(data = train_x, label = train_y, 
                   nfold = 10, 
                   params = params, 
                   watchlist=watchlist,
                   nrounds = 5000, nthread = 8)
Modelbst
##### xgb.cv 10-folds
# iter train_rmse_mean train_rmse_std test_rmse_mean test_rmse_std
# <num>           <num>          <num>          <num>         <num>
# 1      59.0984955    0.160974393      59.082547    1.47087778
# 2      53.2335770    0.144304408      53.220832    1.32844787
# 3      47.9485062    0.130893130      47.937116    1.20817685
# 4      43.1909307    0.118106688      43.182097    1.09257368
# 5      38.9043272    0.106308858      38.894875    0.99560172
# ---                                                                  
# 1496       0.3154410    0.002851323       1.159277    0.07929338
# 1497       0.3152722    0.002862357       1.159236    0.07927931
# 1498       0.3150835    0.002863655       1.159213    0.07924194
# 1499       0.3149020    0.002860694       1.159210    0.07920645
# 1500       0.3147514    0.002864434       1.159220    0.07917070

# finding best hyper parameters

# Define the grid of hyperparameters to search over
grid <- expand.grid(
  eta = c(0.01, 0.1, 0.3),
  max_depth = c(3, 5, 7),
  gamma = c(0, 1, 5),
  colsample_bytree = c(0.6, 0.8, 1),
  min_child_weight = c(1, 5, 10),
  subsample = c(0.6, 0.8, 1),
  nrounds = c(100, 500, 1000)
)

# Split the grid into smaller segments
segments <- split(grid, ceiling(seq_along(1:nrow(grid))/100))  # Adjust the number 100 to change the segment size

# Initialize variables to store the best parameters and the lowest error
best_params <- list()
lowest_error <- Inf

# Perform the grid search over each segment
for(i in seq_along(segments)) {
  for(j in 1:nrow(segments[[i]])) {
    params <- list(
      objective = "reg:squarederror",
      eta = segments[[i]]$eta[j],
      max_depth = segments[[i]]$max_depth[j],
      gamma = segments[[i]]$gamma[j],
      colsample_bytree = segments[[i]]$colsample_bytree[j],
      min_child_weight = segments[[i]]$min_child_weight[j],
      subsample = segments[[i]]$subsample[j],
      nthread = 10
    )
    
    # Perform cross-validation
    cv_model <- xgb.cv(
      params = params,
      data = train_x,
      label = train_y,
      nfold = 10,
      nrounds = segments[[i]]$nrounds[j],
      verbose = 0,
      early_stopping_rounds = 10
    )
    
    # Check if this model is better than the previous best
    if(min(cv_model$evaluation_log$test_rmse_mean) < lowest_error) {
      best_params <- params
      lowest_error <- min(cv_model$evaluation_log$test_rmse_mean)
    }
    # Print the parameters and performance of the current model
    print(paste("Parameters: ", toString(params)))
    print(paste("Test RMSE: ", min(cv_model$evaluation_log$test_rmse_mean)))
  }
  
  # Print the best parameters after each segment
  print(paste("Best parameters so far: ", toString(best_params)))
  print(paste("Best test RMSE so far: ", lowest_error))
}
# 
# [1] "Best parameters so far:  reg:squarederror, 0.01, 5, 1, 1, 10, 0.6, 10"
# [1] "Best test RMSE so far:  1.08109625207776"

best_params <- list(
  objective = "reg:squarederror",
  eta = 0.01,
  max_depth = 5,
  gamma = 1,
  colsample_bytree = 1,
  min_child_weight = 10,
  subsample = 0.6
)

Modelbst <- xgb.cv(data = train_x, label = train_y, 
                   nfold = 10, 
                   params = best_params,
                   nrounds = 5000,
                   nthread = 10,
                   nrepeats = 3)
Modelbst

# ##### xgb.cv 10-folds
# iter train_rmse_mean train_rmse_std test_rmse_mean test_rmse_std
# <num>           <num>          <num>          <num>         <num>
# 1      64.9651813    0.170158021      64.947524    1.52740159
# 2      64.3203650    0.168738243      64.303282    1.51224909
# 3      63.6824357    0.167033053      63.665206    1.49744642
# 4      63.0502907    0.164826640      63.033538    1.48323564
# 5      62.4248858    0.162903889      62.408415    1.46918754
# ---                                                                  
# 4996       0.3948247    0.007022873       1.099612    0.09160705
# 4997       0.3947570    0.007004292       1.099611    0.09161575
# 4998       0.3947089    0.006999146       1.099612    0.09162511
# 4999       0.3946689    0.007000803       1.099610    0.09163714
# 5000       0.3946232    0.006981744       1.099615    0.09163258

# finding the best interation
best_iteration <- which.min(Modelbst$evaluation_log$test_rmse_mean)
print(paste("Best iteration: ", best_iteration))

# Assuming testing_data is your test dataset
test_x = data.matrix(testing_data[,1:85])
test_y = testing_data[,86]

# Assuming you have the full training data (full_train_x, full_train_y)
xgb_train <- xgb.DMatrix(data = train_x, label = train_y)

# Train the final model with the optimal number of boosting rounds
final_model <- xgb.train(params = best_params,
                         data = xgb_train,
                         nrounds = best_iteration,  # Use the optimal value
                         verbose = TRUE)

######################################################################################
# Core Use for caret library
######################################################################################
detectCores()
# Register the parallel backend
registerDoParallel(cores=9)

# Stop parallel processing
stopImplicitCluster()  
######################################################################################
# Caret Library Models
######################################################################################

fitControl <- trainControl(method = "repeatedcv", 
                           number = 10,     # number of folds
                           repeats = 10)    # repeated ten times

modelLR.cv <- train(THA_NextDay_Close ~ .,data = training_data, method = "lm", trControl = fitControl,
                 preProcess = c('scale', 'center')) # default: no pre-processing
modelLR.cv
# RMSE      Rsquared   MAE      
# 1.069043  0.9989329  0.6767594

modelLR.cv <- train(THA_NextDay_Close ~ .,data = training_data, method = "glm", trControl = fitControl,
                 preProcess = c('scale', 'center'))
modelLR.cv
# RMSE      Rsquared   MAE      
# 1.070334  0.9989319  0.6770894

fitControl <- trainControl(method = "repeatedcv", 
                           number = 10,     # number of folds
                           repeats = 10)

modelRF.cv <- train(THA_NextDay_Close ~ .,data = training_data[1:2500,], method = "rf", trControl = fitControl)

modelRF.cv
# mtry  RMSE      Rsquared   MAE      
# 2    1.346805  0.9981960  0.9081782
# 43    1.373797  0.9981046  0.9018582
# 85    1.385005  0.9980735  0.9115734

fitControl <- trainControl(method = "repeatedcv", 
                           number = 10,     # number of folds
                           repeats = 10)

modelRF.cv <- train(THA_NextDay_Close ~ .,data = training_data[1:4000,], method = "rf", trControl = fitControl)

modelRF.cv

# mtry  RMSE      Rsquared   MAE      
# 2    1.108971  0.9989719  0.7063872
# 43    1.135913  0.9989185  0.7093012
# 85    1.143773  0.9989039  0.7164353

fitControl <- trainControl(method = "repeatedcv", 
                           number = 10,     # number of folds
                           repeats = 10)

modelRF.cv <- train(THA_NextDay_Close ~ .,data = training_data[1:5186,], method = "rf", trControl = fitControl)

modelRF.cv
# 
# mtry  RMSE      Rsquared   MAE      
# 2    1.065056  0.9989444  0.6845557
# 43    1.097091  0.9988764  0.6950652
# 85    1.106668  0.9988572  0.7042211

######################################################################################
# Feature Importance - Random Forest
######################################################################################

modelRF_importance <- randomForest(THA_NextDay_Close ~ ., data = training_data[1:5186,], trControl = fitControl,)

# Get feature importance
importance <- importance(modelRF_importance)

# Sort the importance in descending order
importance_sorted <- sort(importance, decreasing = TRUE, index.return = TRUE)

# Get the names of the sorted features
feature_names <- rownames(importance)[importance_sorted$ix]

# Combine the names and importance into a data frame
importance_df <- data.frame(Feature = feature_names, Importance = importance_sorted$x)

# Print the data frame
options(scipen = 999)  # This will disable scientific notation
print(importance_df)

######################################################################################
# Feature Importance - Random Forest Model Training
######################################################################################

# Filter the features with importance above 60
important_features <- importance_df$Feature[importance_df$Importance > 60]

# Create a new training dataset with only the important features
training_data_important <- training_data[1:5186, important_features]

# Add the target variable to the new training dataset
training_data_important$THA_NextDay_Close <- training_data$THA_NextDay_Close[1:5186]

fitControl <- trainControl(method = "repeatedcv", 
                           number = 10,     # number of folds
                           repeats = 10)
modelRF.cv <- train(THA_NextDay_Close ~ .,data = training_data_important[1:5186,], method = "rf", trControl = fitControl)

modelRF.cv

# mtry  RMSE      Rsquared   MAE      
#  2    1.052131  0.9989709  0.6721646
# 31    1.097282  0.9988783  0.6959071
# 60    1.106284  0.9988601  0.7038500

######################################################################################
# compare unseen Test data Random Forest
######################################################################################

#use model to make predictions on test data
pred_y = predict(modelRF.cv, test_x)

#Test Performance
# performance metrics on the test data
caret::RMSE(test_y, pred_y) #rmse - Root Mean Squared Error

# TEST RSME
# [1] 1.012785
##############

pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
#        test_y    pred_y
# 4754  30.58000  29.73852
# 4782  28.71000  29.27385
# 4783  29.13000  29.28360
# 4811  26.09000  26.62205
# 4824  26.16000  26.16478
# 4834  24.72000  25.20768
# 4838  25.71000  25.50832
# 4842  24.89000  24.97314
# 4852  24.65000  24.54762
# 4854  24.70000  24.60044
# 4865  24.00000  24.55215
# 4866  24.30000  25.02568
# 4881  26.85000  26.37367
# 4892  27.68000  27.06381
# 4905  26.87000  27.42953
# 4920  27.05000  26.47123
# 4921  25.72000  25.56448

######################################################################################
# compare unseen Test data XGBoost
######################################################################################

# Make predictions on your test data
xgb_test <- xgb.DMatrix(data = test_x)
test_predictions <- predict(final_model, xgb_test)

# Calculate test RMSE
test_rmse <- sqrt(mean((test_y - test_predictions)^2))
cat(paste("Test RMSE: ", round(test_rmse, 4), "\n"))

# Test RMSE:  1.0779 
####################

# dataframe with actual and predicted values from bestiteration of XGBoost Model
pred_xgb = data.frame(test_y = test_y, pred_y = test_predictions)

# Print the xgboost predictions
pred_xgb

# Last Rows in console
#     test_y    pred_y
# 491  24.89000  25.07238
# 492  24.65000  24.90401
# 493  24.70000  25.08719
# 494  24.00000  24.46511
# 495  24.30000  25.22667
# 496  26.85000  26.21328
# 497  27.68000  27.12252
# 498  26.87000  27.46225
# 499  27.05000  26.70387
# 500  25.72000  25.41287

######################################################################################
# Save the best model --> RandomForest Using Feature Importance
######################################################################################

saveRDS(modelRF.cv, "E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 2\\rfModel.rds")

######################################################################################
# Using Saved Model for Predictions --> RandomForest Using Feature Importance
######################################################################################

savedRF_Model <- readRDS("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 2\\rfModel.rds")

#use model to make predictions on test data
pred_y = predict(savedRF_Model, test_x)

#Test Performance from Saved Model File
# performance metrics on the test data
caret::RMSE(test_y, pred_y) #rmse - Root Mean Squared Error

# RSME
# [1] 1.012785

# Predictions from Saved Model File
pred= cbind.data.frame(test_y,pred_y)
pred
# Last Rows in console
# 4866  24.30000  25.02568
# 4881  26.85000  26.37367
# 4892  27.68000  27.06381
# 4905  26.87000  27.42953
# 4920  27.05000  26.47123
# 4921  25.72000  25.56448