# Databricks notebook source
import openai

print(openai.__version__)

# COMMAND ----------

# MAGIC %pip install openai pydantic

# COMMAND ----------

import openai
import pydantic

print("OpenAI:", openai.__version__)
print("Pydantic:", pydantic.__version__)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

workspace_url = ctx.apiUrl().get()
notebook_token = ctx.apiToken().get()

print(workspace_url)
print("Token retrieved successfully")

# COMMAND ----------

import requests

url = f"{workspace_url}/api/2.0/secrets/scopes/create"

headers = {
    "Authorization": f"Bearer {notebook_token}",
    "Content-Type": "application/json"
}

payload = {
    "scope": "openai",
    "scope_backend_type": "DATABRICKS"
}

response = requests.post(
    url,
    headers=headers,
    json=payload
)

print(response.status_code)
print(response.text)

# COMMAND ----------

import requests
import getpass

# Get Databricks workspace context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

workspace_url = ctx.apiUrl().get()
notebook_token = ctx.apiToken().get()

# Enter your OpenAI API key securely
openai_key = getpass.getpass("Enter your OpenAI API key: ")

url = f"{workspace_url}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {notebook_token}",
    "Content-Type": "application/json"
}

payload = {
    "scope": "openai",
    "key": "api_key",
    "string_value": openai_key
}

response = requests.post(
    url,
    headers=headers,
    json=payload
)

print("Status:", response.status_code)
print("Response:", response.text)

# Remove the key from the Python variable
del openai_key

# COMMAND ----------

dbutils.secrets.list("openai")

# COMMAND ----------

api_key = dbutils.secrets.get(
    scope="openai",
    key="api_key"
)

print("OpenAI API key retrieved successfully")
print(api_key)

# COMMAND ----------

import os
from openai import OpenAI

os.environ["OPENAI_API_KEY"] = dbutils.secrets.get(
    scope="openai",
    key="api_key"
)

client = OpenAI()

# COMMAND ----------

response = client.responses.create(
    model="gpt-5.5",
    input="Hello from my Databricks POC"
)

print(response.output_text)

# COMMAND ----------

import requests

print(requests.__version__)