from typing import List, Union, Generator, Iterator
import openai
import pandas as pd
from pydantic import BaseModel
import requests
from sqlalchemy import create_engine, text
import os

class BusinessExample(BaseModel):
    question: str
    answer: str

class SchemaAnnotation(BaseModel):
    annotation: str
    database_url: str
    database_dialect: str

class QueryRequest(BaseModel):
    question: str
    org_db_id: str
    org_name: str
    business_context: str | None = None
    business_rules: List[str] | None = None
    business_examples: List[BusinessExample] | None = None
    schema_annotation: SchemaAnnotation | None = None

class SchemaAnnotationRequest(BaseModel):
    org_name: str
    org_db_id: str
    database_url: str
    database_dialect: str = "postgresql"

class Pipeline:

    class Valves(BaseModel):
        ORG_NAME: str
        DB_ID: str
        DB_URL: str
        DB_DIALECT: str = "postgresql"
        DB_SCHEMA_DESCRIPTION: str

    def __init__(self):
        # Optionally, you can set the id and name of the pipeline.
        # Best practice is to not specify the id so that it can be automatically inferred from the filename, so that users can install multiple versions of the same pipeline.
        # The identifier must be unique across all pipelines.
        # The identifier must be an alphanumeric string that can include underscores or hyphens. It cannot contain spaces, special characters, slashes, or backslashes.
        # self.id = "wiki_pipeline"
        # self.name = "Organization PTN Employee Pipeline"

        self.T2Q_URL: str = os.getenv("T2Q_BASE_URL", "http://localhost:8000")
        self.T2Q_API_KEY: str = os.getenv("T2Q_API_KEY", "1234567890")

        self.OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.OPENAI_API_BASE_URL: str = os.getenv("OPENAI_API_BASE_URL", "https://api.openai.com/v1")
        self.OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

        self.valves = self.Valves(
            **{
                # "pipelines": ["*"],   # Connect to all pipelines
                # "name": "Organization PTN Employee Pipeline",
                "ORG_NAME": os.getenv("ORG_NAME", "<<Organization Name>>"),
                "DB_ID": os.getenv("DB_ID", "<<db_id>>"),
                "DB_URL": os.getenv("DB_URL", "<<db_url>>"),
                "DB_DIALECT": "postgresql",
                "DB_SCHEMA_DESCRIPTION": os.getenv("DB_SCHEMA_DESCRIPTION", "<<to be updated>>")
            }
        )

        pass

    def init_db_connection(self):
        print(f"init_db_connection:{__name__}")
        print(f"🚀 DB_URL: {self.valves.DB_URL}")
        self.engine = create_engine(f"{self.valves.DB_URL}")
        return self.engine

    async def on_startup(self):

        print(f"on_startup:{__name__}")
        self.update_schema_description()

        pass

    async def on_valves_updated(self):

        print(f"on_valves_updated:{__name__}")

        self.update_schema_description()

    def update_schema_description(self):
        
        print(f"update_schema_description:{__name__}")

        if self.valves.DB_URL != "<<db_url>>" and self.valves.DB_SCHEMA_DESCRIPTION == "<<to be updated>>" or self.valves.DB_SCHEMA_DESCRIPTION == "":

            self.init_db_connection()

            url = f"{self.T2Q_URL}/api/v1/schema_annotation/annotate"

            print(f"🚀 Schema Annotation URL: {url}")

            request = SchemaAnnotationRequest(
                org_name=self.valves.ORG_NAME,
                org_db_id=self.valves.DB_ID,
                database_url=self.valves.DB_URL,
                database_dialect=self.valves.DB_DIALECT,
                ddl_file=""
            )

            try:
                files = {'ddl_file': ('schema.sql', '')}

                params = {
                    'org_name': request.org_name,
                    'org_db_id': request.org_db_id,
                    'database_url': request.database_url,
                    'database_dialect': request.database_dialect
                }

                r = requests.post(
                    url=url,
                    params=params,
                    files=files
                )

                if r.status_code == 200:
                    response_json = r.json()
                    self.valves.DB_SCHEMA_DESCRIPTION = response_json["data"]["annotation"]
                    print(f"🚀 DB_SCHEMA_DESCRIPTION: {self.valves.DB_SCHEMA_DESCRIPTION}")

            except Exception as e:
                print(f"🔥 Error: {e}")
                print(f"🔥 Have to run the t2q composed service to get the schema description")

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
    
    def format_business_rules(self, business_rules: List[str]):
        # Rule 1: , Rule 2: , Rule 3:
        return ", ".join([f"Rule {i+1}: {rule}" for i, rule in enumerate(business_rules)])
    
    def call_openai_chat(self, messages: List[dict]):

        headers = {}
        headers["Authorization"] = f"Bearer {self.OPENAI_API_KEY}"
        headers["Content-Type"] = "application/json"

        payload = {
            "model": self.OPENAI_MODEL,
            "messages": messages,
            "temperature": 0,
            "max_tokens": 10
        }

        try:
            r = requests.post(
                url=f"{self.OPENAI_API_BASE_URL}/chat/completions",
                json=payload,
                headers=headers,
            )

            r.raise_for_status()
            
            response_content = r.json()["choices"][0]["message"]["content"].lower()
            return response_content

        except Exception as e:
            print(f"Error calling OpenAI: {e}")
            return ""
        
    def create_emoji_title(self, user_message: str):

        print(f"🚀 user_message: {user_message}")

        if "Create a concise" in user_message:

            messages = [
                {"role": "user", "content": user_message}
            ]

            return self.call_openai_chat(messages)
        else:
            return None

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
                
        print(f"pipe:{__name__}")

        emoji_title = self.create_emoji_title(user_message)

        if emoji_title:
            print(f"🚀 emoji_title: {emoji_title}")
            return emoji_title


        business_context = body.get("business_context", "")
        business_rules = body.get("business_rules", [])
        few_shot_examples = body.get("examples", [])

        query_request = QueryRequest(
            question=user_message,
            org_db_id=self.valves.DB_ID,
            org_name=self.valves.ORG_NAME,
            business_context=business_context,
            business_rules=business_rules,
            business_examples=[],
            schema_annotation=SchemaAnnotation(
                annotation=self.valves.DB_SCHEMA_DESCRIPTION,
                database_url=self.valves.DB_URL,
                database_dialect=self.valves.DB_DIALECT
            )
        )
        
        business_intent_check = self.business_intent_check(
            user_message=user_message,
            business_context=business_context,
            business_rules=business_rules
        )

        # Check if the question is business-related
        if not business_intent_check:  
            return "We can only answer questions related to related business. Or your question is too vague. Please rephrase your question to focus on business-related topics."
      
        url = f"{self.T2Q_URL}/api/v1/queries/generate"

        r = requests.post(
            url=url,   
            # headers={"X-API-Key": f"{self.T2Q_API_KEY}"},
            json=query_request.model_dump(),
        )

        if r.status_code == 200:
            try:
                response = r.json()

                generated_sql = response["generated_sql"]

                print(f"🚀 generated_sql: {generated_sql}")

                if generated_sql:

                    query = f"```sql\n{generated_sql}\n```"

                    self.init_db_connection()

                    with self.engine.connect() as connection:
                        result = connection.execute(text(generated_sql))
                        final_response = result.fetchall()

                    return f"**Generated SQL Query:**\n {query}\n\n\n**Data Response:**\n {self.format_markdown_results(final_response)}"
                else:
                    return "I wasn't able to translate that into SQL just yet"

            except Exception as e:
                print(f"🔥 Error: Received status code {r.status_code} and error: {e}")
                if generated_sql:
                    return f"This is the generated SQL query: \n {query}\n\n But I couldn't execute it. Please try again."
                else:
                    return "I wasn't able to translate that into SQL just yet - could you provide more context? 😊"
        else:
            return "I wasn't able to translate that into SQL just yet - could you provide more context? 😊"

    def business_intent_check(self, user_message: str, business_context: str, business_rules: str):

        if (business_context == "" and business_rules == []):
            print(f"🚀 No business context or business rules provided")
            return True
        
        business_rules = self.format_business_rules(business_rules)

        BUSINESS_INTENT_CHECK_PROMPT = """
            You are a business expert. You are given a question and business context (schema description, business context) and you need to determine if the question is business-related.
            If it is, return True. If it is not, return False. And not explain anything.

            Question: {USER_MESSAGE}
            Business Context: {BUSINESS_CONTEXT}
            Business Schema Description: {DB_SCHEMA_DESCRIPTION}
            Business Rules: {BUSINESS_RULES}
        """

        BUSINESS_INTENT_CHECK_PROMPT = BUSINESS_INTENT_CHECK_PROMPT.format(
            USER_MESSAGE=user_message,
            BUSINESS_CONTEXT=business_context,
            DB_SCHEMA_DESCRIPTION=self.valves.DB_SCHEMA_DESCRIPTION,
            BUSINESS_RULES=business_rules
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
