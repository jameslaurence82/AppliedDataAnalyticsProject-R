# get min date value from SSIS expression
param([string]$DT)
Write-Host("The UNIX date passed is: " + $DT)

# Get today's date in Unix Epoch
$todayDate = [math]::Round((Get-Date).ToUniversalTime().Subtract((Get-Date "1970-01-01")).TotalSeconds)

# Print the Unix epoch
Write-Host "Today's date in Unix epoch: $todayDate"

# sleep time to ensure python script can complete before dataflow task begins
Start-Sleep -Seconds 3

# period 1 corresponds to min date
# period 2 corresponds to todays date

# ########################################
# ######## Thales Stock Prices: ##########
# ########################################

# # Yahoo Finances uses UNIX timestamp in the URL for dates
# #########################################################

# Thales France Stock Prices (HO.PA) >>>>> EURO!!!<<<<<<<
Write-Host "https://query1.finance.yahoo.com/v7/finance/download/HO.PA?period1=$DT&period2=$todayDate&interval=1d&events=history&includeAdjustedClose=true"

# Fetch the data
$response1 = Invoke-WebRequest -Uri "https://query1.finance.yahoo.com/v7/finance/download/HO.PA?period1=$DT&period2=$todayDate&interval=1d&events=history&includeAdjustedClose=true"

# Convert the response content from a CSV string to objects
$THdata = $response1.Content | ConvertFrom-Csv

# Export the data to a CSV file
$THdata | Export-Csv -Path "E:/5-Data Analytics Winter 2024/DBAS3090 - Applied Data Analytics/Project/dirtyThales_Stock.csv" -NoTypeInformation

########################################
######### Market Indicators: ###########
########################################

# Yahoo Finances uses UNIX timestamp in the URL for dates
#########################################################

# France Stock Market Indicator (^FCHI) >>>>> EURO!!!<<<<<<<

Write-Host "https://query1.finance.yahoo.com/v7/finance/download/%5EFCHI?period1=$DT&period2=$todayDate&interval=1d&events=history&includeAdjustedClose=true"

# Fetch the data
$response2 = Invoke-WebRequest -Uri "https://query1.finance.yahoo.com/v7/finance/download/%5EFCHI?period1=$DT&period2=$todayDate&interval=1d&events=history&includeAdjustedClose=true"

# Convert the response content from a CSV string to objects
$CACdata = $response2.Content | ConvertFrom-Csv

# Export the data to a CSV file
$CACdata | Export-Csv -Path "E:/5-Data Analytics Winter 2024/DBAS3090 - Applied Data Analytics/Project/dirtyFRA_Index.csv" -NoTypeInformation

# S&P 500 Index (^SPX) >>>>> US Dollars!!!<<<<<<<

# Yahoo Finances uses UNIX timestamp in the URL for dates
#########################################################

Write-Host "https://query1.finance.yahoo.com/v7/finance/download/%5ESPX?period1=$DT&period2=$todayDate&interval=1d&events=history&includeAdjustedClose=true"

# Fetch the data
$response3 = Invoke-WebRequest -Uri "https://query1.finance.yahoo.com/v7/finance/download/%5ESPX?period1=$DT&period2=$todayDate&interval=1d&events=history&includeAdjustedClose=true"

# Convert the response content from a CSV string to objects
$SPdata = $response3.Content | ConvertFrom-Csv

# Export the data to a CSV file
$SPdata | Export-Csv -Path "E:/5-Data Analytics Winter 2024/DBAS3090 - Applied Data Analytics/Project/dirtySP500_INDEX.csv" -NoTypeInformation

# Yahoo Finances uses UNIX timestamp in the URL for dates
#########################################################

# Euro Stoxx 50 eIndex >>>>> EURO!!!<<<<<<<

Write-Host "https://query1.finance.yahoo.com/v7/finance/download/%5EGDAXI?period1=$DT&period2=$todayDate&interval=1d&events=history&includeAdjustedClose=true"

# # Fetch the data
$response4 = Invoke-WebRequest -Uri "https://query1.finance.yahoo.com/v7/finance/download/%5EGDAXI?period1=$DT&period2=$todayDate&interval=1d&events=history&includeAdjustedClose=true"

# # Convert the response content from a CSV string to objects
$daxdata = $response4.Content | ConvertFrom-Csv

# # Export the data to a CSV file
$daxdata | Export-Csv -Path "E:/5-Data Analytics Winter 2024/DBAS3090 - Applied Data Analytics/Project/dirtyEURO_Index.csv" -NoTypeInformation

# # run python script after csv files are downloaded
python "E:/5-Data Analytics Winter 2024/DBAS3090 - Applied Data Analytics/Project/dropnull.py"

# sleep time to ensure python script can complete before dataflow task begins
Start-Sleep -Seconds 5