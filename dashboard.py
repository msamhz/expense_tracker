
import pandas as pd
import plotly.express as px
import panel as pn
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Enable Panel extensions
pn.extension('plotly')

# Database connection
DB_CONFIG = {
    "dbname": "FinanceTracker",
    "user": os.getenv("DB_USER", "your_username"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

def load_data():
    engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    query = "SELECT * FROM transactions"
    df = pd.read_sql(query, engine)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])  # Ensure dates are datetime format
    return df

df = load_data()

def income_expense_overview():
    df_summary = df.copy()
    df_summary['transaction_date'] = df_summary['transaction_date'].dt.to_period('M').astype(str)
    df_summary = df_summary.groupby(['transaction_date', 'category'], as_index=False).agg({'amount': 'sum'})

    # Create a dictionary mapping each (month, category) to its top 5 transactions (description & amount)
    hover_metadata = {}
    for (month, category) in df_summary[['transaction_date', 'category']].drop_duplicates().values:
        df_filtered = df[(df['transaction_date'].dt.to_period('M').astype(str) == month) & 
                         (df['category'] == category)]
        df_top = df_filtered.groupby(['description'], as_index=False)['amount'].sum()
        df_top = df_top.sort_values(by='amount', ascending=False).head(5)

        metadata_str = "<br>───────────────<br>"  # Separator line
        metadata_str += "<br>".join([
            f"{row['description']}: ${row['amount']:,.2f}"
            for _, row in df_top.iterrows()
        ])
        hover_metadata[(month, category)] = metadata_str

    # Map the hover data to the summary DataFrame
    df_summary['Top Transactions'] = df_summary.apply(lambda row: hover_metadata.get((row['transaction_date'], row['category']), ""), axis=1)

    # Create stacked bar chart with hover data
    fig = px.bar(df_summary, 
                 x='transaction_date', 
                 y='amount', 
                 color='category',
                 title="Expenses Overview", 
                 width=1200, 
                 barmode='stack',
                 hover_data={'Top Transactions': True})  # Add top transactions in hover data

    return fig

# Create interactive filters for Year and Month
year_options = sorted(df['transaction_date'].dt.year.unique(), reverse=True)
year_filter_cat = pn.widgets.Select(name='Year Filter', options=year_options, value=year_options[0])

month_options = sorted(df['transaction_date'].dt.strftime('%B').unique())  # Get months as names
month_filter_cat = pn.widgets.MultiSelect(name='Month Filter', options=month_options, value=month_options)

@pn.depends(year_filter_cat.param.value, month_filter_cat.param.value)
def category_breakdown(year_filter, month_filter):
    df_filtered = df[df['transaction_date'].dt.year == year_filter]

    # Filter by selected months
    df_filtered = df_filtered[df_filtered['transaction_date'].dt.strftime('%B').isin(month_filter)]

    if df_filtered.empty:
        return pn.pane.Markdown("⚠️ **No data available for the selected filters.**")

    # Aggregate total spending per category
    df_summary = df_filtered.groupby('category', as_index=False)['amount'].sum()

    # Create a dictionary mapping each category to its top 5 transactions (description & amount)
    hover_metadata = {}
    for category in df_summary['category'].unique():
        df_top = df_filtered[df_filtered['category'] == category].groupby('description', as_index=False)['amount'].sum()
        df_top = df_top.sort_values(by='amount', ascending=False).head(5)

        metadata_str = "<br>───────────────<br>"  # Separator line
        metadata_str += "<br>".join([
            f"{row['description']}: ${row['amount']:,.2f}"
            for _, row in df_top.iterrows()
        ])
        hover_metadata[category] = metadata_str

    # Map the hover data to the summary DataFrame
    df_summary['Top Transactions'] = df_summary['category'].map(hover_metadata)

    # Create a pie chart with hover data
    fig = px.pie(df_summary, 
                 names='category', 
                 values='amount', 
                 title=f"Expense Breakdown - {year_filter}", 
                 width=600,
                 hover_data={'Top Transactions': True})  # Add top transactions in hover data

    return fig

# Create interactive filter for months, defaulting to the closest month
month_options = sorted(df['transaction_date'].dt.to_period('M').astype(str).unique(), reverse=True)
closest_month = month_options[0]  # Default to the latest month
month_filter_sub = pn.widgets.Select(name='Month Filter', options=month_options, value=closest_month)

@pn.depends(month_filter_sub.param.value)
def subcategory_breakdown(month_filter):
    df_filtered = df[df['transaction_date'].dt.to_period('M').astype(str) == month_filter]

    # Aggregate total spending per subcategory and category
    df_summary = df_filtered.groupby(['subcategory', 'category'], as_index=False)['amount'].sum()

    # Sort subcategories by highest spending
    df_summary = df_summary.sort_values(by='amount', ascending=False)

    # Create a dictionary mapping each subcategory to its top 5 transactions (description & amount)
    hover_metadata = {}
    for subcat in df_summary['subcategory'].unique():
        df_subcat = df_filtered[df_filtered['subcategory'] == subcat]
        df_top = df_subcat.groupby(['description'], as_index=False)['amount'].sum()
        df_top = df_top.sort_values(by='amount', ascending=False).head(5)

        metadata_str = "<br>───────────────<br>"  # Separator line
        metadata_str += "<br>".join([
            f"{row['description']}: ${row['amount']:,.2f}"
            for _, row in df_top.iterrows()
        ])
        hover_metadata[subcat] = metadata_str

    # Map the hover data to the summary DataFrame
    df_summary['Top Transactions'] = df_summary['subcategory'].map(hover_metadata)

    # Create a grouped bar chart with hover data showing the top transactions
    fig = px.bar(df_summary, 
                 x='subcategory', 
                 y='amount', 
                 color='category',
                 title=f"Subcategory Breakdown - {month_filter}", 
                 barmode='group', 
                 width=1200,
                 hover_data={'Top Transactions': True})  # Add top transactions in hover data

    # Rotate x-axis labels for better readability
    fig.update_xaxes(title_text="Subcategory", tickangle=-45)
    fig.update_yaxes(title_text="Total Amount Spent")

    return fig


def expense_total_heatmap():
    df_filtered = df.copy()

    # Convert transaction_date to period for correct monthly aggregation
    df_filtered['transaction_date'] = df_filtered['transaction_date'].dt.to_period('M').astype(str)

    # Aggregate total expenses per month
    df_summary = df_filtered.groupby('transaction_date', as_index=False)['amount'].sum()

    # Create a dictionary mapping each month to its subcategory expenses with descriptions
    hover_metadata = {}
    for month in df_summary['transaction_date']:
        df_month = df_filtered[df_filtered['transaction_date'] == month]
        df_top = df_month.groupby(['subcategory', 'description'], as_index=False)['amount'].sum()
        df_top = df_top.sort_values(by='amount', ascending=False).head(10)

        # Create hover text with a separator line
        metadata_str = "<br>───────────────<br>"  # Separator line
        metadata_str += "<br>".join([
            f"{row['subcategory']}: {row['description']} (${row['amount']:,.2f})"
            for _, row in df_top.iterrows()
        ])
        hover_metadata[month] = metadata_str

    # Add metadata to DataFrame for hover display
    df_summary['Top Expenses'] = df_summary['transaction_date'].map(hover_metadata)

    # Create heatmap-style bar chart with a green gradient and text labels
    fig = px.bar(df_summary, 
                 x='transaction_date', 
                 y='amount', 
                 title="Total Expenses Across Months", 
                 width=600, 
                 color='amount', 
                 color_continuous_scale='Greens',
                 hover_data={'Top Expenses': True},
                 text=df_summary['amount'].apply(lambda x: f"${x:,.2f}"))  # Display total amount

    # Ensure x-axis labels show only months properly formatted
    fig.update_xaxes(title_text="Month", type='category', tickmode='array', tickvals=df_summary['transaction_date'])
    fig.update_yaxes(title_text="Total Expenses")

    # Position the text labels on top of the bars
    fig.update_traces(textposition='outside')

    return fig

# Create widgets for filtering
category_options = list(df['category'].unique())
category_filter = pn.widgets.Select(name='Category Filter', options=category_options, value=category_options[0])

# Create month range selectors
month_options = sorted(df['transaction_date'].dt.to_period('M').astype(str).unique())
start_month_filter = pn.widgets.Select(name="Start Month", options=month_options, value=month_options[0])
end_month_filter = pn.widgets.Select(name="End Month", options=month_options, value=month_options[-1])

@pn.depends(category_filter.param.value, start_month_filter.param.value, end_month_filter.param.value)
def expenses_over_time(category_filter, start_month_filter, end_month_filter):
    df_filtered = df.copy()

    # Convert transaction_date to period for month filtering
    df_filtered['transaction_date'] = df_filtered['transaction_date'].dt.to_period('M').astype(str)

    # Filter data by selected month range and subcategory
    df_filtered = df_filtered[(df_filtered['transaction_date'] >= start_month_filter) & 
                              (df_filtered['transaction_date'] <= end_month_filter) & 
                              (df_filtered['category'] == category_filter)]

    if df_filtered.empty:
        print("⚠️ Warning: No data found after filtering!")

    # Convert transaction_date back to datetime format
    df_filtered['transaction_date'] = pd.to_datetime(df_filtered['transaction_date'])

    return df_filtered

# Create interactive filter for months, defaulting to the latest month
month_options = sorted(df['transaction_date'].dt.to_period('M').astype(str).unique(), reverse=True)
month_filter = pn.widgets.Select(name='Month Filter', options=month_options, value=month_options[0])

# Panel callback to update the graph dynamically
# Panel callback to update the graph dynamically
@pn.depends(category_filter.param.value, month_filter.param.value)
def update_expenses_over_time(subcategory_filter, month_filter):
    return expenses_over_time(subcategory_filter, month_filter)

@pn.depends(month_filter_sub.param.value)
def update_subcategory_breakdown(month_filter_sub):
    return subcategory_breakdown(month_filter_sub)

@pn.depends(year_filter_cat.param.value, month_filter_sub.param.value)
def update_category_breakdown(year_filter_cat, month_filter_sub):
    return category_breakdown(year_filter_cat, month_filter_sub)

dashboard = pn.Column(
    "# Financial Dashboard",
    pn.Row(
        pn.Column(pn.pane.Markdown("### Select Filters"),year_filter_cat , month_filter_cat),
        pn.Column(pn.pane.Markdown("### Category Breakdown"), pn.panel(category_breakdown)),
        pn.Column(pn.pane.Markdown("### Bank Account Distribution"), pn.panel(expense_total_heatmap))
    ),
    pn.Row(pn.pane.Markdown("### Overview of Income and Expenses"), pn.panel(income_expense_overview, width=1200)),
    pn.Row(pn.pane.Markdown("### Select Month"), month_filter_sub),
    pn.Row(pn.pane.Markdown("### Subcategory Breakdown"), pn.panel(subcategory_breakdown, width=1200)),
    pn.Row(
    pn.Column(
        pn.pane.Markdown("### Filters"),
        category_filter,
        start_month_filter,
        end_month_filter
    ), pn.Column(pn.pane.Markdown("### expense over time"), pn.panel(expenses_over_time)),
    )
)

# Serve the dashboard on localhost
pn.serve(dashboard, address="localhost", port=5006, show=True)