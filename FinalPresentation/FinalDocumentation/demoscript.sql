-- After ETL, Europeans Markets are closed for Monday so April 15th data is incomplete
-- Removing data for prediction	
-- removed the values so i could predict the close price for friday April 12th, 2024
DELETE FROM [dbo].[S_P_INDEX] WHERE FK_DT_Date BETWEEN '2024-04-12' AND '2024-04-15';
DELETE FROM [dbo].[THALES_STOCK] WHERE FK_DT_Date BETWEEN '2024-04-12' AND '2024-04-15';
DELETE FROM [dbo].[EURO_INDEX] WHERE FK_DT_Date BETWEEN '2024-04-12' AND '2024-04-15';
DELETE FROM [dbo].[FRANCE_INDEX] WHERE FK_DT_Date BETWEEN '2024-04-12' AND '2024-04-15';

-- Verify removal of Fridays April 12th from Data for Prediction
SELECT TOP 1 *
  FROM [ThalesStockPredictor].[dbo].[vw_TRAINING_DATA]
  order by FK_DT_Date desc