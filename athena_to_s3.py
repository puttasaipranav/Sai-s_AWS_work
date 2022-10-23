import csv
import io
import boto3
from datetime import date, timedelta, time
from botocore.exceptions import ClientError

today = date.today()
today = today - timedelta(days=2)
today = str(today)

DATABASE = 'gdc-db-cag-dap'
output = 's3://mahi-dl-dataproduct/companion_animal_global/athena_views/PET_OWNER/'
query = 'select * from "gdc-db-cag-dap"."mahus_de_prod_pet_owner" where (cast(row_creation_system_ts as varchar) ='+"""\'"""+today+"""\'"""+ ' or cast(row_update_system_ts as varchar)= '+"""\'"""+today+"""\'"""+')'

output1 = 's3://mahi-dl-dataproduct/companion_animal_global/athena_views/PET/'
query1 = 'select * from "gdc-db-cag-dap"."mahus_de_prod_pet_profile" where (cast(row_creation_system_ts as varchar) ='+"""\'"""+today+"""\'"""+ ' or cast(row_update_system_ts as varchar)= '+"""\'"""+today+"""\'"""+')'

output2 = 's3://mahi-dl-dataproduct/companion_animal_global/athena_views/OPT_IN/'
query2 = 'select * from "gdc-db-cag-dap"."mahus_de_prod_opt_in" where (cast(row_creation_system_ts as varchar) ='+"""\'"""+today+"""\'"""+ ' or cast(row_update_system_ts as varchar)= '+"""\'"""+today+"""\'"""+')'

output3 = 's3://mahi-dl-dataproduct/companion_animal_global/athena_views/SUBSCRIPTION/'
query3 = 'select * from "gdc-db-cag-dap"."mahus_de_prod_subscription" where (cast(row_creation_system_ts as varchar) ='+"""\'"""+today+"""\'"""+ ' or cast(row_update_system_ts as varchar)= '+"""\'"""+today+"""\'"""+')'

output4 = 's3://mahi-dl-dataproduct/companion_animal_global/athena_views/PRODUCT_ORDER/'
query4 = 'select * from "gdc-db-cag-dap"."mahus_de_prod_product_order" where (cast(row_creation_system_ts as varchar) ='+"""\'"""+today+"""\'"""+ ' or cast(row_update_system_ts as varchar)= '+"""\'"""+today+"""\'"""+')'


def lambda_handler(event,context):
    client = boto3.client('athena')
    response = client.start_query_execution(QueryString = query, 
    QueryExecutionContext = {'Database': DATABASE}, 
    ResultConfiguration = {'OutputLocation': output})

   
    response = client.start_query_execution(QueryString = query1, 
    QueryExecutionContext = {'Database': DATABASE}, 
    ResultConfiguration = {'OutputLocation': output1})
    
    response = client.start_query_execution(QueryString = query2, 
    QueryExecutionContext = {'Database': DATABASE}, 
    ResultConfiguration = {'OutputLocation': output2})
    
    response = client.start_query_execution(QueryString = query3, 
    QueryExecutionContext = {'Database': DATABASE}, 
    ResultConfiguration = {'OutputLocation': output3})
   
    response = client.start_query_execution(QueryString = query4, 
    QueryExecutionContext = {'Database': DATABASE}, 
    ResultConfiguration = {'OutputLocation': output4})
