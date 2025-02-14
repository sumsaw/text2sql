from sqlalchemy import text, inspect, create_engine
from sqlalchemy.engine import url


def lambda_handler(event, context):
    
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']
    parameters = event.get('parameters', [])

    def get_schema():
        aws_access_key_id="<ADD ACCESSS KEY FOR AWS ACCOUNT>"
        aws_secret_access_key="<ADD SECRET ACCESS KEY FOR ACCOUNT"
        region_name='us-east-1'
        s3_staging_dir='<YOUR RESULT ATHENA BUCKET>'

        conn_str = (
        f"awsathena+rest://"
        f"{aws_access_key_id}:{aws_secret_access_key}@"
        f"athena.{region_name}.amazonaws.com:443/"
        f"?s3_staging_dir={s3_staging_dir}"
        )

        #create engine 

        engine = create_engine(conn_str)

        # Get the inspector object 
        inspector = inspect(engine)

        schemas  = inspector.get_schema_names()
        db = ""
        for s in schemas:
            schema=f"Database: {s}\n"
            for table_name in inspector.get_table_names(schema=s):
                schema += f"  Table: {table_name}\n"
                for column in inspector.get_columns(table_name,schema=s):
                    col_name = column["name"]
                    col_type = str(column["type"])
                    if column.get("primary_key"):
                        col_type += ", Primary Key"
                    if column.get("foreign_keys"):
                        fk = list(column["foreign_keys"])[0]
                        col_type += f", Foreign Key to {fk.column.table.name}.{fk.column.name}"
                    schema += f"- {col_name}: {col_type}\n"
                schema += "\n"
            db+=schema+"\n"
        return db

    

    if function == 'get_schema':
        try:            
            result_text=get_schema()
            
        except RuntimeError:
            result_text=  "Error : Unable to fetch schema from AWS athena" 
        

    response_body = {
        'TEXT': {
            'body': result_text
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

    
