

#Get Data
honeypurity.data <- read.csv("C:\\Users\\Student\\Desktop\\ML Example\\honey_purity_dataset_AddNulls.csv", stringsAsFactors = TRUE)

#Deal with Pollen_analysis as text and one hot encode
df=honeypurity.data[!is.na(honeypurity.data$G),]
library(caret)
#define one-hot encoding function
dummy <- dummyVars(" ~ Pollen_analysis", data=df)
#perform one-hot encoding on data frame
onehot.pollen <- data.frame(predict(dummy, newdata=df))
#honeypurity.data$Pollen_analysis= as.numeric(factor(honeypurity.data$Pollen_analysis))
df=cbind.data.frame(onehot.pollen,df)
df=subset(df, select = -c(Pollen_analysis))# drop pollen column
# remove nulls
df=honeypurity.data[!is.na(honeypurity.data$G),]

train_idx= sample(1:nrow(df), 0.9 * nrow(df))
train.data= df[train_idx,]

# get test and validation data
test.data= df[-train_idx,]

##############################################
# feature transformation/Engineering
#############################################
library(dplyr)

mutate(df,
       scale_G = scale(G),#zscore
       log_viscosity = log10(Viscosity)
)

library(xgboost)

# Get all the data required
train_x = data.matrix(train.data[,1:10])
train_y = train.data[,11]

test_x = data.matrix(test.data[,1:10])
test_y = test.data[,11]

# now build the model
xgb_train = xgb.DMatrix(data = train_x, label = train_y)
xgb_test = xgb.DMatrix(data = test_x, label = test_y)

#fit XGBoost model and display training and testing data at each iteartion
param <- list(max_depth = 5,
              eta = 0.05,
              gamma = 0.01
              
)

model <- xgboost(data=xgb_train, params = param, nrounds=1500)

## SAVE the model
xgb.save(model, 'C:\\Users\\Student\\Desktop\\ML Example\\best.model')

################################
# Pred Results using saved model
################################
my_savedmodel=xgb.load('C:\\Users\\Student\\Desktop\\ML Example\\best.model')
#use model to make predictions on test data
pred_y = predict(my_savedmodel, test_x)

#Test Performance
# performance metrics on the test data
rmse = caret::RMSE(test_y, pred_y) #rmse - Root Mean Squared Error
rmse
#saveRDS(model_knn, "C:\\Users\\Student\\Documents\\model.rds")
#my_savedmodel <- readRDS("C:\\Users\\Student\\Documents\\model.rds")
pred= cbind.data.frame(test_y,pred_y)
pred


