from typing import TypedDict, Annotated, Union, NotRequired, List
import re

class ConverterState(TypedDict):
    input_query: str
    ast: Annotated[Union[dict, str, None], None]
    translated_sql: Annotated[str, None]
    join_agg_optimized_sql: Annotated[str, None]
    simplified_sql: Annotated[str, None]
    filtered_sql: Annotated[str, None]
    final_optimized_sql: Annotated[str, None]
    optimization_notes: Annotated[str, None]
    final_sql_documentation: Annotated[str, None]
    messages: NotRequired[List[str]]

def parse_final_optimised_query(raw_output):
    # Extract final query and explanation
    query_block = re.search(r'### \[QUERY\]\s+(?:```sql)?\s*(.*?)\s*(?:```)?\s*(?=###|\Z)', raw_output, re.DOTALL)
    explanation_match = re.search(r'### \[EXPLANATION\]\s+(.*)', raw_output, re.DOTALL)

    final_optimized_query = query_block.group(1).strip() if query_block else None
    optimization_notes = explanation_match.group(1).strip() if explanation_match else None
    
    return final_optimized_query, optimization_notes