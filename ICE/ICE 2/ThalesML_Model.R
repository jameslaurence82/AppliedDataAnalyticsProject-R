library(RODBC)
library(caret)
library(dplyr)
library(corrgram)
library(corrplot)
library(randomForest)
library(xgboost)
library(doParallel)
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
testing_data <- initial_test_data[-val_index,]

# remove data split variables
rm(train_index)
rm(val_index)
rm(initial_test_data)
rm(train_prop)
rm(val_prop)

# remove data if issues after split
# rm(training_data)
# rm(testing_data)
# rm(validation_data)

######################################################################################
# NORMALIZE data
######################################################################################

# Build your own `normalize()` function
normalize <- function(x) {
  num <- x - min(x)
  denom <- max(x) - min(x)
  return (num/denom)
}

# Create a vector of column names to exclude
exclude_columns <- c("FK_DT_Date", "THA_NextDay_Close")

# Run normalization on all columns of the dataset (excluding the specified columns)
training_data[, setdiff(names(training_data), exclude_columns)] <- lapply(training_data[, setdiff(names(training_data), exclude_columns)], normalize)

# Run normalization on all columns of the dataset (excluding the specified columns)
validation_data[, setdiff(names(validation_data), exclude_columns)] <- lapply(validation_data[, setdiff(names(validation_data), exclude_columns)], normalize)

# Run normalization on all columns of the dataset (excluding the specified columns)
testing_data[, setdiff(names(testing_data), exclude_columns)] <- lapply(testing_data[, setdiff(names(testing_data), exclude_columns)], normalize)

# remove normalization variables
rm(normalize)
rm(exclude_columns)

# parallel core script 
# # Get the number of CPU cores available
# num_cpu_cores <- detectCores()
# 
# Print the result
cat("Number of CPU cores available:", num_cpu_cores, "\n")

# Set the number of cores (e.g., 2 cores)
num_cores <- 8
cl <- makeCluster(num_cores)

# Register the parallel backend
registerDoParallel(cl)
stopCluster(cl)  # Stop parallel processing

######################################################################################
# Models Training
######################################################################################

modelLR <- train(THA_NextDay_Close ~ .,data = training_data, method = "lm", 
                 preProcess = c('scale', 'center')) # default: no pre-processing
modelLR
# RMSE     Rsquared  MAE      
# 1.10558  0.998864  0.7021597
# 

modelRF <- train(THA_NextDay_Close ~ .,data = training_data, method = "rf",
                 preProcess = 'knnImpute')
modelRF
#   mtry  RMSE      Rsquared   MAE      
#  2    1.171695  0.9987316  0.7510880
# 43    1.146840  0.9987783  0.7296137
# 85    1.163732  0.9987426  0.7437623
# 
# RMSE was used to select the optimal model using the smallest value.
# The final value used for the model was mtry = 43.

modelLR <- train(THA_NextDay_Close ~ .,data = training_data, method = "lm",
                 preProcess = 'YeoJohnson')
modelLR
# RMSE     Rsquared  MAE      
# 1.10558  0.998864  0.7021597

# Train a decision tree model
modelDT <- train(THA_NextDay_Close ~ .,
                 data = training_data,
                 method = "rpart",  # Specify decision tree method
                 trControl = trainControl(method = "cv", number = 100),  # Cross-validation
                 preProcess = c('BoxCox'))  # Pre-processing (optional)
modelDT
# cp          RMSE       Rsquared   MAE      
# 0.03846636   8.862407  0.9244864   7.021761
# 0.09208200  12.190283  0.8605919   9.703239
# 0.81539661  24.097196  0.8008478  20.187928
# 
# RMSE was used to select the optimal model using the smallest value.
# The final value used for the model was cp = 0.03846636.

# Extract features and labels
train_x <- data.matrix(training_data[, 1:10])
train_y <- training_data[, 11]

valid_x <- data.matrix(validation_data[, 1:10])
valid_y <- validation_data[, 11]

test_x <- data.matrix(testing_data[, 1:10])
test_y <- testing_data[, 11]

# Create DMatrix for training and validation
xgb_train <- xgb.DMatrix(data = train_x, label = train_y)
xgb_valid <- xgb.DMatrix(data = valid_x, label = valid_y)

# Set up watchlist for monitoring training progress
watchlist <- list(train = xgb_train, test = xgb_valid)

# Specify hyperparameters and train the model
params <- list(
  objective = "reg:squarederror",  # Regression task
  max_depth = 3,                  # Maximum depth of trees
  eta = 0.1,                      # Learning rate (step size)
  gamma = 0,                      # Regularization parameter
  colsample_bytree = 0.8,         # Fraction of features to consider
  min_child_weight = 1,           # Minimum sum of instance weight
  subsample = 0.8                 # Fraction of samples to consider
)

modelXGB1 <- xgb.train(
  data = xgb_train,
  params = params,
  watchlist = watchlist,
  nrounds = 5000
)

# [4983]	train-rmse:0.000665	test-rmse:0.005541 
# [4984]	train-rmse:0.000665	test-rmse:0.005542 
# [4985]	train-rmse:0.000665	test-rmse:0.005542 
# [4986]	train-rmse:0.000665	test-rmse:0.005542 
# [4987]	train-rmse:0.000665	test-rmse:0.005542 
# [4988]	train-rmse:0.000665	test-rmse:0.005541 
# [4989]	train-rmse:0.000665	test-rmse:0.005541 
# [4990]	train-rmse:0.000665	test-rmse:0.005541 
# [4991]	train-rmse:0.000665	test-rmse:0.005542 
# [4992]	train-rmse:0.000665	test-rmse:0.005541 
# [4993]	train-rmse:0.000664	test-rmse:0.005541 
# [4994]	train-rmse:0.000664	test-rmse:0.005541 
# [4995]	train-rmse:0.000664	test-rmse:0.005541 
# [4996]	train-rmse:0.000664	test-rmse:0.005541 
# [4997]	train-rmse:0.000664	test-rmse:0.005541 
# [4998]	train-rmse:0.000664	test-rmse:0.005541 
# [4999]	train-rmse:0.000664	test-rmse:0.005541 
# [5000]	train-rmse:0.000664	test-rmse:0.005541 

modelLR <- train(THA_NextDay_Close ~ .,data = training_data, method = "glm",
                 preProcess = c('scale', 'center'))
modelLR
