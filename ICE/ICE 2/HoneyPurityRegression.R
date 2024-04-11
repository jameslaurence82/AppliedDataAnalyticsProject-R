

#Get Data
honeypurity.data <- read.csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 2\\honey_purity_dataset_AddNulls.csv", stringsAsFactors = TRUE)

#Deal with Pollen_analysis as text
honeypurity.data$Pollen_analysis= as.numeric(factor(honeypurity.data$Pollen_analysis))
# remove nulls
df=honeypurity.data[!is.na(honeypurity.data$G),]

###############################
#    investigate data
##############################
# Sort asc/desc
# correlation matrix
# stats/ na values


# Correlation
# http://www.sthda.com/english/wiki/visualize-correlation-matrix-using-correlogram
#install.packages("corrgram")
#install.packages("corrplot")
#library(corrgram)
#corrgram(df, order=TRUE)
#cor(df$Price, df$Purity)  
library(corrplot)
M=cor(df)
corrplot(M, method="number")


# view stats
summary(df)
library(pastecs)
stat.desc(df)



#set seed to save input sample for reproducible results
set.seed(1234)

#Remove data for testing
# Generate a random numbers that is 80% of the total number of rows in dataset.
train_idx= sample(1:nrow(df), 0.9 * nrow(df))
train.data= df[train_idx,]

# get test and validation data
test_Valid.data= df[-train_idx,]
# split into test and validation sets
 validate_idx = sample(1:nrow(test_Valid.data), 0.5 * nrow(test_Valid.data)) 
 validation.data=test_Valid.data[validate_idx,]
 test.data= test_Valid.data[-validate_idx,]


# clear vars before model build
rm(honeypurity.data)
rm(df)
rm(train_idx)
#rm(test_Valid.data)
rm(validate_idx)

# Model data

# What is a good RMSE for regression?
  # By considering the scale of the dependent variable and the magnitude of the RMSE value,
  # we can interpret the effectiveness of our regression model. 
  # For example, if the final exam score ranges from 0 to 100, an RMSE of 4 
  # indicates our model's predictions are pretty accurate, with an average error rate of only 4%.


#   SI= (RMSE/average observed value)*100% => Scatter Index (SI<10% = Good, SI<5% Very Good, SI<30% is acceptable )
# e.g. 27.23/594.8085 * 100 = 4.577%

# Ashrae standard: R2 higher than 0.75 and a SI below 30%

#  r2 shows how well the data fit the regression model (the goodness of fit)

# 1. use LM to determine best data preprocessing - use subset of data
# 2. get var importance using xgboost, rf, nnet, lasso
# 3. run k-fold with 2 repeat

library(caret)

modelLR <- train(Price ~ .,data = train.data[1:10000,], method = "lm", 
                       preProcess = c('scale', 'center')) # default: no pre-processing
modelLR
#RMSE: 210.258 => data subset:  210.0144

modelLR <- train(Price ~ .,data = train.data[1:10000,], method = "lm", 
                 preProcess = 'pca') # default: no pre-processing
modelLR
#RMSE: data subset:  209.77


modelLR <- train(Price ~ .,data = train.data[1:10000,], method = "lm", 
                 preProcess = 'BoxCox')

modelLR
#RMSE: data subset:  209.6941

modelLR <- train(Price ~ .,data = train.data[1:10000,], method = "lm", 
                 preProcess = 'YeoJohnson') 

modelLR
#RMSE: data subset:  210.004

modelLR <- train(Price ~ .,data = train.data[1:10000,], method = "lm", 
                 preProcess = 'bagImpute') 

modelLR
#RMSE: data subset:   209.8008

modelLR <- train(Price ~ .,data = train.data[1:10000,], method = "lm", 
                 preProcess = 'knnImpute') 

modelLR
#RMSE: data subset:  209.2091


# Find hyperparas
# Here we generate a dataframe with a column named lambda with 100 values that goes from 10^10 to 10^-2
lambdaGrid <- expand.grid(lambda = 10^seq(10, -10, length=100))
modelLasso <- train(Price ~ .,data = train.data[1:1000,!(names(train.data) %in% rm_cols)], method = "ridge", 
                    preProcess = 'knnImpute',tuneGrid = lambdaGrid,   # Test all the lambda values in the lambdaGrid dataframe
                    na.action = na.omit)   # Ignore NA values 

modelLasso
#RMSE: data subset:  213.4987


modelRF <- train(Price ~ .,data = train.data[1:1000,], method = "rf", 
                 preProcess = 'knnImpute') 
#RMSE: data subset: 217.7121  
modelRF


########################################################
# Feature Importance
########################################################
ggplot(varImp(modelRF))

rm_cols= c('G','F','Density','EC') # based on VarImp
# rm_cols= c('G','F','Viscosity','EC') # based on correlation coeff
modelRF <- train(Price ~ .,data = train.data[1:1000,!(names(train.data) %in% rm_cols)], method = "rf", 
                 preProcess = 'knnImpute') 

modelRF
#RMSE: data subset:  27.22896

#####################
# Tuning Algo
#####################
# https://topepo.github.io/caret/available-models.html
# hyperparas .mtry, ntree

# mtry 2-3x #vars 
# mtry : the number of variables to randomly sample as candidates at each split

#repGrid = expand.grid(.mtry=c(6, 8, 10, 14,21))
repGrid = expand.grid(.mtry=seq(10, 30))
modelRF = train(Price ~ .,data = train.data[1:1000,!(names(train.data) %in% rm_cols)], method = "rf", 
                 preProcess = 'knnImpute',
                         metric="RMSE",
                tuneGrid = repGrid)



modelRF # RSME: 26.35863

modelRF = train(Price ~ .,data = train.data[1:1000,!(names(train.data) %in% rm_cols)], method = "rf", 
                preProcess = 'knnImpute',
                metric="RMSE",.mtry=19)

modelRF # RSME: 

# https://xgboost.readthedocs.io/en/stable/R-package/discoverYourData.html
# https://www.projectpro.io/recipes/apply-xgboost-r-for-regression

install.packages("xgboost", repos="http://dmlc.ml/drat/", type = "source")

library(xgboost)

# Get all the data required
train_x = data.matrix(train.data[,1:10])
train_y = train.data[,11]

valid_x = data.matrix(validation.data[,1:10])
valid_y = validation.data[,11]

test_x = data.matrix(test.data[,1:10])
test_y = test.data[,11]


# now build the model
xgb_train = xgb.DMatrix(data = train_x, label = train_y)
xgb_valid = xgb.DMatrix(data = valid_x, label = valid_y)

watchlist = list(train=xgb_train, test=xgb_valid)

#fit XGBoost model and display training and testing data at each iteartion
model = xgb.train(data = xgb_train, max.depth = 3, watchlist=watchlist, nrounds = 1000)
# rsme: 1.655402

importance = xgb.importance(feature_names = colnames(xgb_train), model = model)
imp=cbind(importance$Feature,importance$Gain)
imp

#define final model
#model_xgboost = xgboost(data = xgb_train, max.depth = 3, nrounds = 1000, verbose = 0)
#summary(model_xgboost)

#################################
# compare unseen Test data
################################

#use model to make predictions on test data
pred_y = predict(model, test_x)

#Test Performance
# performance metrics on the test data
caret::RMSE(test_y, pred_y) #rmse - Root Mean Squared Error
pred= cbind.data.frame(test_y,pred_y)
pred


