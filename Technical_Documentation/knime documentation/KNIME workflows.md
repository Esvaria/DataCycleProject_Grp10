# KNIME Workflows

This document contains more detailed explanations of the various workflows developed.

## Summary

The work with KNIME was divided into three different workflows: one for analysis, used to work on the data, prepare the final tables and train the models, and two others, published on the KNIME server as a REST API service, because most of the data was processed with a granularity of five minutes, while some, due to the rarity of the events, had a granularity greater than one hour.

The three workflows are:

1. **KNIME_DB_analysis** for data cleaning and model creation.
2. **KNIME_DB** for the REST API service with variables with 5-minute granularity.
3. **KNIME_DB_1H** for the REST API service with variables with 1-hour granularity.

## KNIME_DB_analysis

Most of the work was done on this workflow, initially working locally on the virtual machine (VM) and then transferring everything to the server to finalise the table writing and model training. We primarily used KNIME_DB_analysis to retrieve the data, clean it, process it, and then use it.

### Data loading and processing ###

The data is loaded into the workflow directly from the database, which receives data from the machines every 5 minutes via a Microsoft SQL Server Connector node, allowing us to work as if we were accessing the DB directly. The different tables that make up the database were extracted so that we could identify which ones contained data useful for our analysis. Once the four tables to work on had been identified, we moved on to an initial analysis of the data to understand which data was needed.

In order to aggregate the identified data, it was first necessary to filter the columns of the various tables and then work on the timestamps of this data, as we wanted to preserve the temporality of the events for subsequent analysis while maintaining a granularity of 5 minutes. The timestamps were thus broken down into numbers indicating the year, month, day, hours and minutes, the latter being rounded using mathematical nodes to ensure compatibility and then recomposed.

This allowed us to have a table with all the data aggregated by machine ID and timestamp.

### Identification of the best model and training ###

The data we had already collected was cleaned up by removing anything that wasn't needed for the analysis and fixing any gaps. Then we picked a variable to test the different models, created time series with the lag column node, and ran the tests.

The models tested by performing different variations of the parameters and time series were:

1. Linear Regression
2. Simple Regression Tree
3. Random Forest Regressor
4. RProp MLP

The results showed that the best algorithm in this case was RProp MLP which, being able to simulate a Recurrent Neural Network, was able to capture temporal patterns better than the others, thus producing more accurate predictions. 

Once the model to be used had been identified, a model was trained for each variable on which to produce a forecast.

## KNIME_DB

The actual trained models and data tables produced have been placed on the Knime server so that they can be easily reused by the two workflows that expose the REST APIs.

This workflow, which is responsible for exposing data with 5-minute granularity, uses a container input table node to retrieve the previously produced table and model reader nodes to retrieve the trained models. 

Since the data has already been processed, the workflow is more streamlined than the previous one but retains all the necessary preprocessing steps such as normalisation and data partitioning. 

The results produced by the various predictors are then aggregated by machine ID and timestamp to produce a final table that will be exposed as JSON by the output table container node.

## KNIME_DB_1H

This workflow is responsible for exposing data with 1-hour granularity and is the same as KNIME_DB but uses data from a different table.

## Conclusions ##


To summarise the predictive workflow was developed with KNIME and published on KNIME Server as a REST API service, enabling direct integration with external systems (e.g. Power BI) and inference automation. The architecture adopted separates the data preparation phase from the predictive phase, ensuring modularity and control:

- A separate workflow or SQL script queries the Data Warehouse (DWmachines), processes the data and populates an intermediate table containing the aggregated and filtered data needed for inference. The workflow is also used to test different models and train the chosen one.
- The workflow published on KNIME Server uses a Container Input (Table) node to receive this input data, reading directly from the generated table.
- Within the workflow, the data undergoes a preprocessing phase, then is passed to a multiple regression model capable of simulating a Recurrent Neural Network to estimate multiple target values and identify patterns.
- The model results are passed to a Container Output (Table), allowing:

  - the return of data via API in tabular format,
  - or writing to a file or output table in the database, accessible by business intelligence tools such as Power BI.

This configuration allows you to:

- perform inferences on always up-to-date data (thanks to the table prepared upstream),
- call the model via API when needed,
- easily integrate forecasts into the analytics dashboard.

