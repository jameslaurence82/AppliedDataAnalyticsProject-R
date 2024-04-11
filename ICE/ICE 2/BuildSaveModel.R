# USE the Caret Library to build a ML Prediction Model
install.packages("caret")




iris <- read.csv(url("http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"), 
                 header = FALSE) 

# Add column names
names(iris) <- c("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", "Species")


library(caret)
# CARET TRAINING
# Create index to split based on labels  
index <- createDataPartition(iris$Species, p=0.75, list=FALSE)

# Subset training set with index
iris.training <- iris[index,]

# Subset test set with index
iris.test <- iris[-index,]

model_knn <- train(iris.training[, 1:4], iris.training[, 5], method='knn')
predictions<-predict.train(object=model_knn,iris.test[,1:4])

confusionMatrix(predictions, factor(iris.test[,5]))


saveRDS(model_knn, "C:\\Users\\Student\\Documents\\model.rds")

my_savedmodel <- readRDS("C:\\Users\\Student\\Documents\\model.rds")
predictions<-predict.train(object=my_savedmodel,iris.test[,1:4])
confusionMatrix(predictions, factor(iris.test[,5]))


##################################################################################################
# Code to pass to PowerBI
######
# My prediction "New" data
iris <- read.csv(url("http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"), 
                 header = FALSE) 

# Add column names
names(iris) <- c("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", "Species")


library(caret)
index <- createDataPartition(iris$Species, p=0.75, list=FALSE)
iris.test <- iris[-index,]

####
# Pred Results
my_savedmodel <- readRDS("C:\\Users\\Student\\Documents\\model.rds")
predictions<-predict.train(object=my_savedmodel,iris.test[,1:4])

myPred= as.data.frame(cbind(iris.test[,1:4],predictions))

###########################################
cmx <- as.data.frame(table(predictions, factor(iris.test[,5])))
cm=table(predictions, factor(iris.test[,5]))
accuracy <- as.data.frame(sum(cm[1], cm[4]) / sum(cm[1:4]))

