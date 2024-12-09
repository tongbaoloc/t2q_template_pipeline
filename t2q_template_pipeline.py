from typing import List, Union, Generator, Iterator
import openai
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
    schema_description: str = ""
class ExtractSchemaRequest(BaseModel):
    database_url: str
    schema_id: str

class Pipeline:

    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str        
        DB_DATABASE: str
        DB_SCHEMA_DESCRIPTION: str
        # BUSINESS_CONTEXT: str
        # BUSINESS_RULES: str
        # EXAMPLE_LINK: str

    def __init__(self):
        # Optionally, you can set the id and name of the pipeline.
        # Best practice is to not specify the id so that it can be automatically inferred from the filename, so that users can install multiple versions of the same pipeline.
        # The identifier must be unique across all pipelines.
        # The identifier must be an alphanumeric string that can include underscores or hyphens. It cannot contain spaces, special characters, slashes, or backslashes.
        # self.id = "wiki_pipeline"
        self.name = "Organization PTN Employee Pipeline"

        self.T2Q_URL: str = f"{os.getenv("T2Q_BASE_URL", "http://localhost:8000")}"
        self.T2Q_API_KEY: str = os.getenv("T2Q_API_KEY", "1234567890")

        self.ICL_TYPE: str = os.getenv("ICL_TYPE", "zero_shot")
        self.OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.OPENAI_API_BASE_URL: str = os.getenv("OPENAI_API_BASE_URL", "https://api.openai.com/v1")
        self.OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

        self.valves = self.Valves(
            **{
                "pipelines": ["*"],   # Connect to all pipelines
                "name": "Organization PTN Employee Pipeline",                                                         
                "DB_HOST": os.getenv("DB_HOST", "<<db_host>>"),
                "DB_PORT": os.getenv("DB_PORT", "5432"),
                "DB_USER": os.getenv("DB_USER", "<<db_user>>"),
                "DB_PASSWORD": os.getenv("DB_PASSWORD", "<<db_password>>"),
                "DB_DATABASE": os.getenv("DB_DATABASE", "<<db_database>>"),
                "DB_SCHEMA_DESCRIPTION": "to be updated",
                # "BUSINESS_CONTEXT": "<<business_context>>",
                # "BUSINESS_RULES": "<<business_rules>>",
                # "EXAMPLE_LINK": "<<example_link>>",
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
        
        self.init_db_connection()

        if self.valves.DB_DATABASE:

            url = f"{self.T2Q_URL}/v1/t2q/extract-schema"

            print(f"🚀 url: {url}")

            request = ExtractSchemaRequest(
                database_url= f"postgresql://{self.valves.DB_USER}:{self.valves.DB_PASSWORD}@{self.valves.DB_HOST}:{self.valves.DB_PORT}/{self.valves.DB_DATABASE}",
                schema_id=self.valves.DB_DATABASE,
            )

            r = requests.post(
                url=url,   
                headers={"X-API-Key": f"{self.T2Q_API_KEY}"},
                json=request.model_dump(),
            )

            if r.status_code == 200:
                print(f"🚀 DB_SCHEMA_DESCRIPTION: {self.valves.DB_SCHEMA_DESCRIPTION}")
                self.valves.DB_SCHEMA_DESCRIPTION = str(r.json())

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
        
        business_intent_check = self.business_intent_check(user_message)

        print(f"🚀 business_intent_check: {business_intent_check}")
        
        # Check if the question is business-related
        if not business_intent_check:
            return "I can only answer questions related to business and employee data. Please rephrase your question to focus on business-related topics."

        business_context = body.get("business_context", "")
        business_rules = body.get("business_rules", [])
        examples = body.get("examples", [])

        translate_form = TranslateForm(
            human_question=user_message,
            db_id=self.valves.DB_DATABASE,
            icl_type=self.ICL_TYPE,
            business_context=business_context,
            business_rules=business_rules,
            business_examples=examples,
            schema_description=self.valves.DB_SCHEMA_DESCRIPTION,
        )

        url = f"{self.T2Q_URL}/v1/t2q/translate"

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
                    
                    sql = text(predicted_sql_query)
                    
                    with self.engine.connect() as connection:
                        result = connection.execute(sql)
                        final_response = result.fetchall()

                    return f"**Generated SQL Query:**\n {query}\n\n\n**Data Response:**\n {self.format_markdown_results(final_response)}"
                else:
                    return "I wasn't able to translate that into SQL just yet - could you try rephrasing your question? 😊"

            except requests.exceptions.JSONDecodeError as e:
                print(f"🔥 Error: Received status code {r.status_code} and error: {e}")
                return "I wasn't able to translate that into SQL just yet - could you try rephrasing your question? 😊"

        else:
            print(f"🔥 Error: Received status code {r.status_code} and error: {r.text}")
            return "I wasn't able to translate that into SQL just yet - could you try rephrasing your question? 😊"

    def business_intent_check(self, user_message: str):

        BUSINESS_INTENT_CHECK_PROMPT = """
            You are a business expert. You are given a question and business context (schema description, business context) and you need to determine if the question is business-related.
            If it is, return True. If it is not, return False. And not explain anything.

            Question: {USER_MESSAGE}
            Business Context: {BUSINESS_CONTEXT}
            Business Schema Description: {DB_SCHEMA_DESCRIPTION}
        """

        BUSINESS_INTENT_CHECK_PROMPT = BUSINESS_INTENT_CHECK_PROMPT.format(
            USER_MESSAGE=user_message,
            BUSINESS_CONTEXT="",
            DB_SCHEMA_DESCRIPTION=self.valves.DB_SCHEMA_DESCRIPTION,
        )

        print(f"🚀 BUSINESS_INTENT_CHECK_PROMPT: {BUSINESS_INTENT_CHECK_PROMPT}")

        headers = {}
        headers["Authorization"] = f"Bearer {self.OPENAI_API_KEY}"
        headers["Content-Type"] = "application/json"

        payload = {
            "model": self.OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": "You are a business expert. Respond only with true or false."},
                {"role": "user", "content": BUSINESS_INTENT_CHECK_PROMPT}
            ],
            "temperature": 0,
            "max_tokens": 10  # Increased from 1 to allow for response
            # Remove response_format as it's not needed and causing issues
        }

        try:
            r = requests.post(
                url=f"{self.OPENAI_API_BASE_URL}/chat/completions",  # Use the base URL from config
                json=payload,
                headers=headers,
            )

            r.raise_for_status()
            
            response_content = r.json()["choices"][0]["message"]["content"].lower()
            return "true" in response_content  # More flexible check for true/false response

        except Exception as e:
            print(f"🔥 Error in business_intent_check: {str(e)}")
            return True  # Fallback to allow the query to proceed
