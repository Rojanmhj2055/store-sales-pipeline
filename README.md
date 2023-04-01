# store-sales-pipeline

Data pipeline which cleans and transform the sales data to make it ready for reporting.

## Model Design

 Flowchart for the purposed model

 ```mermaid
 graph LR
    data_proc[Read & clean input file]
    save_clean[Save cleansed data into MYSQL table]
    report_location[Location wise profit]
    report_store[Store wise profit]
    result_csv[Save results into CSV]
    email[Send Email]
    data_proc --> save_clean -->report_location --> result_csv
    save_clean --> report_store --> result_csv
  
    result_csv --> email
 
 ```
