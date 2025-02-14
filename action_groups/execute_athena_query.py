import boto3
import time
import json
import logging
import pandas as pd 

logging.basicConfig(level=logging.INFO)

def lambda_handler(event,context):
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']
    parameters = event.get('parameters', [])

    param_dict= {param['name'].lower() : param['value'] for param in parameters}

    def execute_query(query, database, output_location):

        athena = boto3.client('athena')

        try:
            response = athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': database
                },
                ResultConfiguration={
                    'OutputLocation': output_location
                }
            )
            query_execution_id = response['QueryExecutionId']
            logging.info(f'Query execution ID: {query_execution_id}')
            while True:
                response = athena.get_query_execution(QueryExecutionId=query_execution_id)
                status = response['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)
            if status == 'SUCCEEDED':
                logging.info('Query execution succeeded')
                response = athena.get_query_results(QueryExecutionId=query_execution_id)
               
                column_names = [col['VarCharValue'] for col in response['ResultSet']['Rows'][0]['Data']]
                data_rows = response['ResultSet']['Rows'][1:]
                query_result = [[col['VarCharValue'] for col in row['Data']] for row in data_rows]
                df = pd.DataFrame(query_result, columns=column_names)
                print(f"query_result:{df}")
              
                
                return json.dumps({
                    'statusCode': 200,
                    "body": df.to_dict(orient='records')
                })
            else:
                logging.error(f'Query execution failed with status {status}')
                return json.dumps({
                    'statusCode': 400,
                    "body": status
                })
        except Exception as e:
            logging.error(f'Error executing query: {e}')
            return {
                    'statusCode': 400,
                    "body": e
                }
    if function=="execute_query":
        query = param_dict.get('query')
        database = param_dict.get('database')
        output_location = "<ADD YOUR ATHENA S# LOCATION>"  ### TODO

        result=execute_query(query=query,database=database,output_location=output_location)
    
    print(f"result:{result}")

    response_body = {
    'TEXT': {
        'body': result
    }
    }

    function_response = {
        'actionGroup': event['actionGroup'],
        'function': event['function'],
        'functionResponse': {
            'responseBody': response_body
        }
    }
    
    session_attributes = event['sessionAttributes']
    prompt_session_attributes = event['promptSessionAttributes']
    
    action_response = {
        'messageVersion': '1.0', 
        'response': function_response,
        'sessionAttributes': session_attributes,
        'promptSessionAttributes': prompt_session_attributes
    }
        
    return action_response
        

    




