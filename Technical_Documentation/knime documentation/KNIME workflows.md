# KNIME Workflows

This document contains more detailed explanations of the various workflows developed.

## Summary

The work with KNIME was divided into three different workflows: one for analysis, used to work on the data, prepare the final tables and train the models, and two others, published on the KNIME server as a REST API service, because most of the data was processed with a granularity of five minutes, while some, due to the rarity of the events, had a granularity greater than one hour.

The three workflows are:

1. **KNIME_DB_analysis** for data cleaning and model creation.
2. **KNIME_DB** for the REST API service with variables with 5-minute granularity.
3. **KNIME_DB_1H** for the REST API service with variables with 1-hour granularity.

## KNIME_DB_analysis

Explanation

## KNIME_DB

Explanation

## KNIME_DB_1H

Explanation



