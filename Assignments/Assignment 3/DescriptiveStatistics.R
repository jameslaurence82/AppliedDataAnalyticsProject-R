library(RODBC)
library(caret)
library(dplyr)
library(ggplot2)
library(scales)

######################################################################################
# connect to sql server DB thalesstockpredictor to import THALES_STOCK table
######################################################################################

# Sql server connection string
connStr <- "Driver=SQL Server;Server=MSI;Database=ThalesStockPredictor;trusted_connection=yes"

# establish connection to SQL Server
dbconnection <- odbcDriverConnect(connStr)

# query view vw_IMPORTANCE_RF (Random Forest Feature Importance View) from sql server
queryThalesTable <- "SELECT * 
              FROM THALES_STOCK
              ORDER BY FK_DT_Date desc" # sorted with newest date

# assign the queryData statement from SQL and assign to R dataframe for modeling
SQLThalesTable <- sqlQuery(dbconnection, queryThalesTable)

# close sql server connection
odbcClose(dbconnection)

# remove variables
rm(connStr)
rm(dbconnection)
rm(queryThalesTable)
######################################################################################
# assign SQL data to THALES Dataframe
######################################################################################

# Ensure all columns in Thales_Table that you're summarizing are numeric
Thales_Table <- SQLThalesTable

# remove SQL dataframe
rm(SQLThalesTable)

summary(Thales_Table)



######################################################################################
# Create Gaussian Curve for Closing Values
######################################################################################
# 
# # Calculate mean and standard deviation for closing price
# mean_close <- mean(Thales_Table$THALES_Close, na.rm = TRUE)
# sd_close <- sd(Thales_Table$THALES_Close, na.rm = TRUE)
# 
# # Function to plot histogram and Gaussian curve
# plot_histogram_with_gaussian <- function(data, mean, sd, xlab, title) {
#   # Create a sequence of x values for Gaussian curve
#   x_values <- seq(min(data, na.rm = TRUE), max(data, na.rm = TRUE), length.out = 300)
#   # Compute y values for Gaussian curve
#   y_values <- dnorm(x_values, mean = mean, sd = sd)
#   
#   # Plot the histogram with Gaussian curve
#   p <- ggplot() +
#     geom_histogram(aes(x = data), binwidth = sd/2, colour = "black", fill = "white") +
#     geom_line(aes(x = x_values, y = y_values * length(data) * sd / 2), colour = "blue") +
#     scale_x_continuous(labels = comma) +
#     labs(title = title, x = xlab, y = "Density") +
#     theme_minimal()
#   
#   return(p)
# }
# 
# # Plot histogram and Gaussian curve for closing price
# p_close <- plot_histogram_with_gaussian(
#   Thales_Table$THALES_Close,
#   mean_close,
#   sd_close,
#   xlab = "Closing Price",
#   title = "Distribution of Closing Price with Gaussian Curve"
# )
# 
# # Print the plots
# print(p_close)
# 
# ######################################################################################
# # Create Gaussian Curve for Volume Values
# ######################################################################################
# 
# # Calculate mean and standard deviation for volume
# mean_volume <- mean(Thales_Table$THALES_Volume, na.rm = TRUE)
# sd_volume <- sd(Thales_Table$THALES_Volume, na.rm = TRUE)
# 
# # Function to plot histogram and Gaussian curve
# plot_histogram_with_gaussian <- function(data, mean, sd, xlab, title) {
#   # Create a sequence of x values for Gaussian curve
#   x_values <- seq(min(data, na.rm = TRUE), max(data, na.rm = TRUE), length.out = 300)
#   # Compute y values for Gaussian curve
#   y_values <- dnorm(x_values, mean = mean, sd = sd)
#   
#   # Plot the histogram with Gaussian curve
#   p <- ggplot() +
#     geom_histogram(aes(x = data), binwidth = sd/2, colour = "black", fill = "white") +
#     geom_line(aes(x = x_values, y = y_values * length(data) * sd / 2), colour = "blue") +
#     scale_x_continuous(labels = comma) +
#     labs(title = title, x = xlab, y = "Density") +
#     theme_minimal()
#   
#   return(p)
# }
# 
# 
# # Plot histogram and Gaussian curve for volume
# p_volume <- plot_histogram_with_gaussian(
#   Thales_Table$THALES_Volume,
#   mean_volume,
#   sd_volume,
#   xlab = "Volume",
#   title = "Distribution of Volume with Gaussian Curve"
# )
# 
# # Print the plots
# print(p_volume)