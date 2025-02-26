import os
import pandas as pd
import ollama
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
import concurrent.futures
from tqdm import tqdm
import yaml 





# Load params.yaml
with open("params.yaml", "r") as file:
    params = yaml.safe_load(file)
    
MAX_WORKERS = params["llm_inference"]["max_workers"]
MODEL_NAME = params["llm_inference"]["model_name"]

# Load environment variables
load_dotenv()

# Define the Prompt Template
def generate_prompt(description):
  prompt_template = f"""
  ### Instruction:
  You are a financial AI assistant that classifies financial transactions into predefined categories and subcategories. Your task is to analyze a given transaction description and determine the most appropriate category and subcategory from the following list:

  ### Allowed Categories & Subcategories:
  - **Food & Dining**
    - Eat Out
    - Grab Food
    - Work Lunch (Lunch hours Mon-Fri)

  - **Transportation**
    - Grab Car
    - GetGo Rental Car
    - Car Refuel

  - **Shopping**
    - Online Shopping
    - Retail Shop
    - Groceries (e.g., NTUC, Cold Storage)

  - **Utilities**
    - Mobile Phone
    - Cash Card

  - **Others**
    - Sport
    - Leisure
    - Gifts
    - Household Items (Furniture, TV, Water Dispenser)
    - Renovation Works

  ### **Few-Shot Examples for Learning:**

  #### **Example 1**
  **Transaction Description:** "McDonald's - Lunch Set Meal"
  **Step 1: Identify Category:** McDonald's is a food outlet → **Food & Dining**
  **Step 2: Identify Subcategory:** Lunch hours between Mon-Fri suggest a work-related meal → **Work Lunch**
  **Final Answer:** `Food & Dining - Work Lunch`

  #### **Example 2**
  **Transaction Description:** "Shell Petrol Station - Refuel"
  **Step 1: Identify Category:** Shell is a fuel provider → **Transportation**
  **Step 2: Identify Subcategory:** Fuel purchase implies vehicle refueling → **Car Refuel**
  **Final Answer:** `Transportation - Car Refuel`

  #### **Example 3**
  **Transaction Description:** "Shopee Singapore - Electronics Purchase"
  **Step 1: Identify Category:** Shopee is an online shopping platform → **Shopping**
  **Step 2: Identify Subcategory:** Electronics purchase falls under e-commerce → **Online Shopping**
  **Final Answer:** `Shopping - Online Shopping`

  ### **Now classify the following transaction:**
  **Transaction Description:** "{description}"
  **Step 1: Identify Category:**  
  **Step 2: Identify Subcategory:**  
  **Final Answer:** 
  """
  return prompt_template.strip()

# Define the function to send requests to DeepSeek
def infer_category(description: str) -> str:
    logger = get_run_logger()
    
    """
    Sends a single description to DeepSeek for categorization.
    """
    prompt = generate_prompt(description)
    
    response = ollama.chat(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}])
    logger.info(f'processed: {response["message"]["content"].strip()}')
  
    return response["message"]["content"].strip()

@task
def batch_infer_categories(descriptions: list):
    """
    Processes multiple descriptions in parallel using ThreadPoolExecutor.
    Adjusts worker count dynamically for efficiency.
    """
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for result in tqdm(executor.map(infer_category, descriptions), total=len(descriptions), desc="Processing Inferences", unit="txn"):
            results.append(result)
    
    return results

@flow(name="Async LLM Inference with DeepSeek")
def llm_inference(df: pd.DataFrame):
    """
    Runs DeepSeek inference asynchronously to categorize transactions.
    """
    if df.empty:
        print("⚠️ No data to insert.")
        return

    try:
        descriptions = df["description"].tolist()
        
        # Run async inference in batch
        category_predictions = batch_infer_categories(descriptions)
        
        df["category_prediction"] = category_predictions
        print("✅ Categorization Completed")
        
        return df
    
    except Exception as e:
        print("Error:", e)
