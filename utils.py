from typing import TypedDict, Annotated, Union, NotRequired, List
import json

class ConverterState(TypedDict):
    input_query: str
    ast: Annotated[Union[dict, str, None], None]
    translated_sql: Annotated[str, None]
    efficient_sql: Annotated[str, None]
    messages: NotRequired[List[str]]

def parse_efficient_query_json(json_string):
    # Remove any markdown code block indicators if present
    clean_json = json_string.strip()
    if clean_json.startswith("```json"):
        clean_json = clean_json[7:]
    if clean_json.endswith("```"):
        clean_json = clean_json[:-3]
        
    # Parse the JSON
    data = json.loads(clean_json)
    
    # Extract the relevant information
    result = {
        'steps': data.get('Steps', []),
        'sql_query': data.get('ANSI SQL Query', '')
    }
    
    return result