# dbt Project - Testing and Transforming OLIST ecommerce dataset

I defined schema as the name of folders in the model
used generic test, defined custom generic test (not_negative) as well as singular test for validating the coordinates
defined materialization in the global level

The key in dbt project is to realize and use the source and ref functions correctly, always keep in mind the LAG, the order of data transformation and desired tests etc. 

Link: [Dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

