from typing import List, Union, Generator, Iterator
import pandas as pd
from pydantic import BaseModel
import requests
from sqlalchemy import create_engine, text
import os

class BusinessExample(BaseModel):
    question: str
    sql_query: str

class TranslateForm(BaseModel):
    human_question: str
    db_id: str
    icl_type: str = ""
    business_context: str = ""
    business_rules: List[str] = []
    business_examples: List[BusinessExample] = []

class Pipeline:

    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str        
        DB_DATABASE: str
        BUSINESS_CONTEXT: str
        BUSINESS_RULES: str
        EXAMPLE_LINK: str

    def __init__(self):
        # Optionally, you can set the id and name of the pipeline.
        # Best practice is to not specify the id so that it can be automatically inferred from the filename, so that users can install multiple versions of the same pipeline.
        # The identifier must be unique across all pipelines.
        # The identifier must be an alphanumeric string that can include underscores or hyphens. It cannot contain spaces, special characters, slashes, or backslashes.
        # self.id = "wiki_pipeline"
        self.name = "<<pipeline_name>>"

        self.T2Q_URL: str = f"{os.getenv("T2Q_BASE_URL", "http://localhost:8000")}"
        self.T2Q_API_KEY: str = os.getenv("T2Q_API_KEY", "1234567890")

        self.ICL_TYPE: str = os.getenv("ICL_TYPE", "zero_shot")
        self.OPENAI_API_BASE_URL: str = os.getenv("OPENAI_API_BASE_URL", "https://api.openai.com/v1")
        self.OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "<<openai_api_key>>")

        self.valves = self.Valves(
            **{
                "pipelines": ["*"],   # Connect to all pipelines
                "name": "<<pipeline_name>>",                                                         
                "DB_HOST": os.getenv("DB_HOST", "<<db_host>>"),
                "DB_PORT": os.getenv("DB_PORT", "5432"),
                "DB_USER": os.getenv("DB_USER", "<<db_user>>"),
                "DB_PASSWORD": os.getenv("DB_PASSWORD", "<<db_password>>"),
                "DB_DATABASE": os.getenv("DB_DATABASE", "<<db_database>>"),
                "BUSINESS_CONTEXT": "<<business_context>>",
                "BUSINESS_RULES": "<<business_rules>>",
                "EXAMPLE_LINK": "<<example_link>>",
            }
        )

        pass

    def init_db_connection(self):
        self.engine = create_engine(f"postgresql://{self.valves.DB_USER}:{self.valves.DB_PASSWORD}@{self.valves.DB_HOST}:{self.valves.DB_PORT}/{self.valves.DB_DATABASE}")
        return self.engine

    async def on_startup(self):
        self.init_db_connection()
        pass

    async def on_valves_updated(self):
        print(f"on_valves_updated:{__name__}")
        pass

    async def on_shutdown(self):
        print(f"on_shutdown:{__name__}")
        pass

    def format_markdown_results(self, results):
        if not results:
            return "No results found"
        
        # Get column names from first result
        if hasattr(results[0], '_fields'):  # For results from SQLAlchemy queries
            headers = results[0]._fields
        else:  # For regular tuples
            headers = [f"Column {i+1}" for i in range(len(results[0]))]
        
        # Create header row
        markdown = "| " + " | ".join(str(header) for header in headers) + " |\n"
        # Create separator row
        markdown += "|-" + "-|-".join("-" * len(header) for header in headers) + "-|\n"
        # Create data rows
        for row in results:
            markdown += "| " + " | ".join(str(value) for value in row) + " |\n"
            
        return markdown

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        
        # curl -X 'POST' \
        # 'http://localhost:8000/v1/t2q/translate' \
        # -H 'accept: application/json' \
        # -H 'X-API-Key: 1234567890' \
        # -H 'Content-Type: application/json' \
        # -d '{
        # "human_question": "many cars",
        # "db_id": "car_1",
        # "icl_type": "zero_shot",
        # "business_context": "",
        # "business_rules": [],
        # "business_examples": []
        # }'

        translate_form = TranslateForm(
            human_question=user_message,
            db_id=self.valves.DB_DATABASE,
            icl_type=self.ICL_TYPE,
            business_context=self.valves.BUSINESS_CONTEXT,
            business_rules=[],
            business_examples=[],
        )

        # url = self.T2Q_URL.replace("[question]", user_message).replace("[icl_type]", self.ICL_TYPE).replace("[db_id]", self.valves.DB_DATABASE).replace("[business_context]", self.valves.BUSINESS_CONTEXT)
        url = f"{self.T2Q_URL}/v1/t2q/translate"

        print(f"🔥 url: {url}")

        r = requests.post(
            url=url,   
            headers={"X-API-Key": f"{self.T2Q_API_KEY}"},
            json=translate_form.model_dump(),
        )

        if r.status_code == 200:
            try:
                response = r.json()

                predicted_sql_query = response["predicted_sql_query"]

                if predicted_sql_query:
                    
                    query = f"```sql\n{predicted_sql_query}\n```"
                    
                    # Convert the SQL string to a SQLAlchemy text object
                    sql = text(predicted_sql_query)
                    
                    with self.engine.connect() as connection:
                        result = connection.execute(sql)
                        final_response = result.fetchall()

                    return f"**Generated SQL Query:**\n {query}\n\n\n**Data Response:**\n {self.format_markdown_results(final_response)}"
                else:
                    return "Cannot translate question to SQL query"

            except requests.exceptions.JSONDecodeError as e:
                print(f"🔥 Error: Received status code {r.status_code} and error: {e}")
                return "Cannot translate question to SQL query"
        else:
            print(f"🔥 Error: Received status code {r.status_code}")
            return "Cannot translate question to SQL query"
