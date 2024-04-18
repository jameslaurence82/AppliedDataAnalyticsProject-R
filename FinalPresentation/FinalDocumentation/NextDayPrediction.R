library(RODBC)
library(caret)
library(randomForest)

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
queryTrainingData <- "SELECT TOP (1) [FK_DT_Date]
      ,[THA_Close]
      ,[SPI_MACD]
      ,[FRA_RSI_180]
      ,[SPI_RSI_7]
      ,[FRA_RSI_30]
      ,[THA_High]
      ,[FRA_MACD]
      ,[THA_RSI_180]
      ,[EUR_RSI_7]
      ,[THA_Low]
      ,[SPI_MA_90]
      ,[THA_Volume]
      ,[FRA_LowerBand]
      ,[SPI_LowerBand]
      ,[EUR_UpperBand]
      ,[SPI_Close]
      ,[THA_MACD]
      ,[SPI_EMA26]
      ,[EUR_RSI_30]
      ,[SPI_UpperBand]
      ,[SPI_Open]
      ,[EUR_EMA12]
      ,[SPI_Adj_Close]
      ,[SPI_MA_30]
      ,[EUR_RSI_180]
      ,[SPI_RSI_30]
      ,[FRA_RSI_7]
      ,[SPI_OBV]
      ,[THA_RSI_7]
      ,[THA_Open]
      ,[SPI_EMA12]
      ,[EUR_MA_7]
      ,[EUR_OBV]
      ,[EUR_Low]
      ,[FRA_Adj_Close]
      ,[THA_OBV]
      ,[THA_RSI_90]
      ,[THA_Adj_Close]
      ,[FRA_MA_90]
      ,[SPI_RSI_90]
      ,[EUR_Adj_Close]
      ,[THA_MA_7]
      ,[SPI_High]
      ,[EUR_RSI_90]
      ,[SPI_MA_7]
      ,[EUR_Volume]
      ,[FRA_High]
      ,[FRA_Close]
      ,[SPI_SMA]
      ,[FRA_MA_7]
      ,[FRA_Volume]
      ,[EUR_High]
      ,[EUR_MA_180]
      ,[FRA_Open]
      ,[THA_RSI_30]
      ,[FRA_SMA]
      ,[THA_UpperBand]
      ,[FRA_MA_180]
      ,[THA_EMA12]
      ,[SPI_RSI_180]
      ,[THA_NextDay_Close]
  FROM [ThalesStockPredictor].[dbo].[vw_IMPORTANCE_RF]
  ORDER BY [FK_DT_Date] DESC" # sorted with newest date


# query Test_RMSE Table from SQL Server to get best RMSE Value
queryRMSE <- "SELECT TOP 1 MODEL_RMSE
              FROM [dbo].[MODEL_RMSE]
              ORDER BY MODEL_TimeStamp DESC" # sorted with newest date


# assign the queryData statement from SQL and assign to R dataframe for modeling
Predictor_Data <- sqlQuery(dbconnection, queryTrainingData)

# close sql server connection
odbcClose(dbconnection)

#Remove unneed values and variables
rm(connStr)
rm(queryTrainingData)
rm(queryRMSE)
rm(dbconnection)

######################################################################################
# Prep Next Day Prediction Data for predictive Machine Learning
######################################################################################

# Extract the date value from the FK_DT_Date column
date_value <- as.Date(Predictor_Data$FK_DT_Date[1], format = "%Y-%m-%d")

# change date column to be number date (UNIX Epoch)
Predictor_Data$FK_DT_Date <- as.numeric(as.POSIXct(Predictor_Data$FK_DT_Date))

######################################################################################
# Load Saved Model and Perform Prediction
######################################################################################

# Load Model
rfmodel <- readRDS("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 4\\rfModel.rds")

# Create a new testing dataset with only the important features
test_x <- Predictor_Data

# Define test_y
test_y <- Predictor_Data$THA_NextDay_Close

# Use model to make predictions on test data
pred_y = predict(rfmodel, test_x)

# Display prediction
pred_y <- as.data.frame(pred_y)

#Add date to the Prediction Dataframe
# Add one day to the date value
next_day <- date_value + 1

# Convert the next day value to a character string
next_day <- as.character(next_day)

# Add Date to the prediction dataframe
pred_y$next_day <- next_day

pred_y
# Basic summary
model_summary <- capture.output(print(rfmodel))

# Convert summary to dataframe
model_summary_df <- data.frame(Summary = model_summary, stringsAsFactors = FALSE)
