import pandas as pd
import ollama
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
import concurrent.futures
from tqdm import tqdm
import yaml 
import re 
import json
import time
import functools

from src.config import config

# Load params.yaml
with open(config['PARAMS_FILE'], "r") as file:
    params = yaml.safe_load(file)
    
MAX_WORKERS = params["llm_inference"]["max_workers"]
MODEL_NAME = params["llm_inference"]["model_name"]

# Define function to generate structured prompts
def generate_prompt(description: str) -> str:
    return f"""
    ### Instruction:
    You are a financial AI assistant that classifies financial transactions into predefined categories and subcategories. Your task is to analyze a given transaction description and determine the most appropriate category and subcategory from the following list.

    ### Allowed Categories & Subcategories:
    ### Categories are in ** xxx ** while subcategories are under categories in bullet points.
    ### There should only be 
    - **Food & Dining**
        - Eat Out
        - Grab Food (usually has grab in the description)
        - Work Lunch (Lunch hours Mon-Fri - please look at the transaction date and determine if its on the weekday, usually Koufu, Kopitiam, Food Court, Hawker Centre or other food places)

    - **Transportation**
        - Grab Car/Taxi
        - GetGo Rental Car
        - Car Refuel (Shell, Esso, SPC, Caltex)
        - Public Transport (MRT, Bus, SMRT, SBS)

    - **Groceries**
        - Groceries (NTUC, Cold Storage, Giant, Sheng Siong)
    
    - **Shopping**
        - Online Shopping (e.g., Shopee, Lazada, Amazon, Taobao)    
        - Retail Shop (e.g., Uniqlo, H&M, Watsons, IKEA, Challenger, Popular, Tangs, Bookstore, Gain City, Courts, Harvey Norman, Best Denki, Challenger, Mustafa, Decathlon, Miniso, Daiso, Typo, Cotton On, Muji, Sephora, Guardian, Watsons)
        - Department Store to be considered as "Retail Shop" (e.g., Takashimaya, Isetan, Metro, BHG, OG, Robinsons)
        
    - **Utilities**
        - Mobile Phone (M1, Singtel, Starhub)
        - Cash Card (NetsFlashPay, EZ-Link)
        - Internet & Cable TV
        - Electricity & Gas (SP Services)
        - Subscriptions (e.g., Netflix, Spotify, Github, AWS)
        - Other payments (AXS, SAM)
        
    - **Others**
        - Sport
        - Leisure
        - Gifts
        - Household Items (Furniture, TV, Water Dispenser)
        - Renovation Works
        - Medical (Clinic, Pharmacy, Doctors)
        - Insurance (Prudential (called Pru), AIA, Aviva)
        - Investments
        - Renovation vendors (Hydroflux marketing, knockknock, azora, C P lighting)


    ### **Few-Shot Examples for Learning (Format Consistency)**

    #### **Example 1**
    **Transaction Description:** "McDonald's - Lunch Set Meal"
    **Classification:**
    ```json
    {{
        "category": "Food & Dining",
        "subcategory": "Work Lunch"
    }}
    ```

    #### **Example 2**
    **Transaction Description:** "Shell Petrol Station - Refuel"
    **Classification:**
    ```json
    {{
        "category": "Transportation",
        "subcategory": "Car Refuel"
    }}
    ```

    #### **Example 3**
    **Transaction Description:** "Shopee Singapore - Electronics Purchase"
    **Classification:**
    ```json
    {{
        "category": "Shopping",
        "subcategory": "Online Shopping"
    }}
    ```

    ### **Now classify the following transaction:**
    **Transaction Description:** "{description}"
    
    **Output the JSON response in the exact format below. NO ADDITIONAL TEXT!**
    ```json
    {{
        "category": "<CATEGORY>",
        "subcategory": "<SUBCATEGORY>"
    }}
    ```
    """

def parse_llm_response(response: str):
    """
    Extracts the category and subcategory from the LLM response.
    Returns a tuple (category, subcategory).
    """
    # Try extracting JSON portion from the response
    json_match = re.search(r'```json\n(.*?)\n```', response, re.DOTALL)
    
    if json_match:
        json_str = json_match.group(1)
        try:
            json_response = json.loads(json_str)
            category = json_response.get("category", "Uncategorized")
            subcategory = json_response.get("subcategory", "Uncategorized")
            return category.strip(), subcategory.strip()
        except json.JSONDecodeError:
            pass  # Fall back to regex-based extraction

    # If JSON fails, try regex-based extraction
    match = re.search(r'"category": "(.*?)",\s*"subcategory": "(.*?)"', response)
    
    if match:
        category, subcategory = match.groups()
        return category.strip(), subcategory.strip()
    
    return "Uncategorized", "Uncategorized"

def infer_category(description: str):
    """
    Sends a transaction description to LLM and ensures JSON-formatted output.
    """
    prompt = generate_prompt(description)
    response = ollama.chat(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        options={"temperature": 0.1}
    )

    # Extract JSON response
    try:
        json_response = response["message"]["content"].strip()
        category, subcategory = parse_llm_response(json_response)

        # Ensure valid category and subcategory (fallback to "Uncategorized" if empty)
        category = category if category else "Uncategorized"
        subcategory = subcategory if subcategory else "Uncategorized"

    except (json.JSONDecodeError, KeyError, TypeError):
        category, subcategory = "Uncategorized", "Uncategorized"

    return description, json_response, category, subcategory  # Include description for alignment

@task
def batch_infer_categories(descriptions: list):
    """
    Processes multiple descriptions in parallel using ThreadPoolExecutor.
    Ensures results remain in original order.
    """
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(infer_category, desc): (i, desc) for i, desc in enumerate(descriptions)}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing Inferences", unit="txn"):
            try:
                i, description = futures[future]  # Retrieve original index
                description, json_response, category, subcategory = future.result()
            except Exception as e:
                i, description = futures[future]
                description, json_response, category, subcategory = "Unknown", "{}", "Uncategorized", "Uncategorized"
            
            results.append((i, description, json_response, category, subcategory))
    
    # **Sort results by original index to restore order**
    results.sort(key=lambda x: x[0])  # Sort by `i`

    # Return ordered results without the index
    return [(desc, json_resp, cat, subcat) for _, desc, json_resp, cat, subcat in results]


@task
def time_it(func):
    """
    Decorator to measure execution time of any function.
    """
    logger = get_run_logger()
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)  # Run the actual function
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"✅ {func.__name__} completed in {elapsed_time:.2f} seconds")
        return result
    return wrapper

@flow(name="LLM Inference") 
@time_it 
def llm_inference(df: pd.DataFrame):
    """
    Runs DeepSeek inference asynchronously to categorize transactions.
    """
    if df.empty:
        print("⚠️ No data to process.")
        return

    try:
        descriptions = df["description"].tolist()
        
        # Run async inference in batch
        results = batch_infer_categories(descriptions)

        # Convert results to DataFrame columns
        descp, json_responses, categories, subcategories = zip(*results)
        df["category"] = categories
        df["subcategory"] = subcategories

        return df
    
    except Exception as e:
        print("Error:", e)