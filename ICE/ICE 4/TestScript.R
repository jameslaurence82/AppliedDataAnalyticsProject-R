# Get the current date and time
current_time <- Sys.time()

# Create a message
message <- paste("R Script executed successfully at", current_time, "\n")

# Specify the log file path (adjust the path as necessary)
log_file <- "E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\ICE\\ICE 4\\TestLog.txt"

# Write the message to the log file
write(message, file = log_file, append = TRUE)
