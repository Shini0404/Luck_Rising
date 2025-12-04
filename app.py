"""
================================================================================
TASK 1: INVENTORY HARMONIZATION PIPELINE - STREAMLIT DASHBOARD
================================================================================
Beautiful interactive dashboard for the Unified Product & Inventory 
Data Harmonization Pipeline.

Run with: streamlit run app.py
================================================================================
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime
import yaml

# Import our pipeline
from etl.task1_inventory_pipeline import InventoryHarmonizationPipeline

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Inventory Harmonization Pipeline",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# CUSTOM CSS STYLING
# =============================================================================

st.markdown("""
<style>
    /* Main theme colors */
    :root {
        --primary-color: #1E3A5F;
        --secondary-color: #3D5A80;
        --accent-color: #00D4FF;
        --success-color: #00C853;
        --warning-color: #FFB300;
        --error-color: #FF5252;
        --bg-dark: #0E1117;
        --bg-card: #1E2530;
    }
    
    /* Header styling */
    .main-header {
        background: linear-gradient(135deg, #1E3A5F 0%, #3D5A80 50%, #00D4FF 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        text-align: center;
        box-shadow: 0 10px 30px rgba(0, 212, 255, 0.2);
    }
    
    .main-header h1 {
        color: white;
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }
    
    .main-header p {
        color: #E0E0E0;
        font-size: 1.1rem;
        margin-top: 0.5rem;
    }
    
    /* Metric cards */
    .metric-card {
        background: linear-gradient(145deg, #1E2530 0%, #2A3441 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border-left: 4px solid #00D4FF;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        margin-bottom: 1rem;
    }
    
    .metric-card h3 {
        color: #00D4FF;
        font-size: 2rem;
        margin: 0;
        font-weight: 700;
    }
    
    .metric-card p {
        color: #B0BEC5;
        margin: 0.5rem 0 0 0;
        font-size: 0.9rem;
    }
    
    /* Status badges */
    .status-success {
        background: linear-gradient(135deg, #00C853, #00E676);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: 600;
        display: inline-block;
    }
    
    .status-warning {
        background: linear-gradient(135deg, #FFB300, #FFC107);
        color: #1E1E1E;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: 600;
        display: inline-block;
    }
    
    .status-error {
        background: linear-gradient(135deg, #FF5252, #FF1744);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: 600;
        display: inline-block;
    }
    
    /* Layer cards */
    .layer-card {
        background: linear-gradient(145deg, #1A237E 0%, #283593 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        border: 1px solid #3949AB;
    }
    
    .layer-raw {
        border-left: 4px solid #CD7F32;
    }
    
    .layer-staging {
        border-left: 4px solid #C0C0C0;
    }
    
    .layer-curated {
        border-left: 4px solid #FFD700;
    }
    
    .layer-quarantine {
        border-left: 4px solid #FF5252;
    }
    
    /* Pipeline flow */
    .pipeline-flow {
        display: flex;
        justify-content: space-around;
        align-items: center;
        padding: 1rem;
        background: linear-gradient(90deg, #1E2530, #2A3441);
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .flow-step {
        text-align: center;
        padding: 1rem;
    }
    
    .flow-arrow {
        color: #00D4FF;
        font-size: 2rem;
    }
    
    /* Data tables */
    .dataframe {
        font-size: 0.85rem !important;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background: linear-gradient(180deg, #1E2530 0%, #0E1117 100%);
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Custom button */
    .stButton > button {
        background: linear-gradient(135deg, #00D4FF 0%, #0099CC 100%);
        color: white;
        font-weight: 600;
        border: none;
        padding: 0.75rem 2rem;
        border-radius: 25px;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 20px rgba(0, 212, 255, 0.4);
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

@st.cache_data
def load_curated_data():
    """Load curated inventory fact data."""
    path = "curated/inventory_fact/inventory_fact.csv"
    if os.path.exists(path):
        return pd.read_csv(path)
    return None

@st.cache_data
def load_quarantine_data():
    """Load quarantine data."""
    inventory_path = "quarantine/quarantine_inventory_snapshot/quarantine_inventory_snapshot.csv"
    restock_path = "quarantine/quarantine_restock_events/quarantine_restock_events.csv"
    
    data = {}
    if os.path.exists(inventory_path):
        data['inventory'] = pd.read_csv(inventory_path)
    if os.path.exists(restock_path):
        data['restock'] = pd.read_csv(restock_path)
    
    return data

@st.cache_data
def load_raw_data():
    """Load raw source data."""
    data = {}
    if os.path.exists("raw/inventory_snapshot.csv"):
        data['inventory_snapshot'] = pd.read_csv("raw/inventory_snapshot.csv")
    if os.path.exists("raw/restock_events.csv"):
        data['restock_events'] = pd.read_csv("raw/restock_events.csv")
    if os.path.exists("raw/products.csv"):
        data['products'] = pd.read_csv("raw/products.csv")
    if os.path.exists("raw/stores.csv"):
        data['stores'] = pd.read_csv("raw/stores.csv")
    return data

def load_config():
    """Load pipeline configuration."""
    with open("config/task1_config.yml", 'r') as f:
        return yaml.safe_load(f)

# =============================================================================
# MAIN APP
# =============================================================================

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>📦 Inventory Harmonization Pipeline</h1>
        <p>Unified Product & Inventory Data - Single Source of Truth</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/000000/data-configuration.png", width=80)
        st.title("Navigation")
        
        page = st.radio(
            "Select Page",
            ["🏠 Dashboard", "▶️ Run Pipeline", "📊 Data Explorer", "🔍 Quarantine Audit", "⚙️ Configuration"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.markdown("### Pipeline Info")
        config = load_config()
        st.info(f"**Version:** {config['pipeline']['version']}")
        st.info(f"**Name:** {config['pipeline']['name']}")
        
        st.markdown("---")
        st.markdown("### Quick Stats")
        curated = load_curated_data()
        if curated is not None:
            st.success(f"✅ Curated Records: {len(curated)}")
        else:
            st.warning("⚠️ No curated data yet")
    
    # Page routing
    if page == "🏠 Dashboard":
        show_dashboard()
    elif page == "▶️ Run Pipeline":
        show_run_pipeline()
    elif page == "📊 Data Explorer":
        show_data_explorer()
    elif page == "🔍 Quarantine Audit":
        show_quarantine_audit()
    elif page == "⚙️ Configuration":
        show_configuration()


def show_dashboard():
    """Main dashboard view."""
    st.header("📊 Pipeline Dashboard")
    
    # Load data
    curated = load_curated_data()
    quarantine = load_quarantine_data()
    raw = load_raw_data()
    
    if curated is None:
        st.warning("⚠️ No pipeline data found. Please run the pipeline first!")
        if st.button("🚀 Run Pipeline Now"):
            show_run_pipeline()
        return
    
    # Pipeline Flow Visualization
    st.subheader("🔄 Pipeline Flow")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown("""
        <div style="text-align:center; padding:1rem; background:linear-gradient(145deg,#8B4513,#A0522D); border-radius:10px;">
            <h3 style="color:#FFD700;">🥉 RAW</h3>
            <p style="color:white;">Bronze Layer</p>
            <h2 style="color:white;">{}</h2>
            <p style="color:#FFD700;">Records</p>
        </div>
        """.format(len(raw.get('inventory_snapshot', [])) + len(raw.get('restock_events', []))), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""<div style="text-align:center; font-size:3rem; padding-top:2rem;">➡️</div>""", unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="text-align:center; padding:1rem; background:linear-gradient(145deg,#708090,#A9A9A9); border-radius:10px;">
            <h3 style="color:#1E1E1E;">🥈 STAGING</h3>
            <p style="color:#1E1E1E;">Silver Layer</p>
            <h2 style="color:#1E1E1E;">Validated</h2>
            <p style="color:#1E1E1E;">& Cleaned</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""<div style="text-align:center; font-size:3rem; padding-top:2rem;">➡️</div>""", unsafe_allow_html=True)
    
    with col5:
        st.markdown("""
        <div style="text-align:center; padding:1rem; background:linear-gradient(145deg,#FFD700,#FFA500); border-radius:10px;">
            <h3 style="color:#1E1E1E;">🥇 CURATED</h3>
            <p style="color:#1E1E1E;">Gold Layer</p>
            <h2 style="color:#1E1E1E;">{}</h2>
            <p style="color:#1E1E1E;">Records</p>
        </div>
        """.format(len(curated)), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Key Metrics
    st.subheader("📈 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Curated Records",
            value=f"{len(curated):,}",
            delta="Ready for Analytics"
        )
    
    with col2:
        quarantine_count = sum(len(df) for df in quarantine.values())
        st.metric(
            label="Quarantine Records",
            value=f"{quarantine_count:,}",
            delta="Invalid Records",
            delta_color="inverse"
        )
    
    with col3:
        avg_stock = curated['effective_stock_level'].mean() if 'effective_stock_level' in curated.columns else 0
        st.metric(
            label="Avg Effective Stock",
            value=f"{avg_stock:,.0f}",
            delta="Units"
        )
    
    with col4:
        total_movement = curated['stock_movement'].sum() if 'stock_movement' in curated.columns else 0
        st.metric(
            label="Total Stock Movement",
            value=f"{total_movement:,.0f}",
            delta="Net Change"
        )
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📦 Effective Stock by Store")
        if 'store_name' in curated.columns and 'effective_stock_level' in curated.columns:
            store_stock = curated.groupby('store_name')['effective_stock_level'].sum().reset_index()
            fig = px.bar(
                store_stock,
                x='store_name',
                y='effective_stock_level',
                color='effective_stock_level',
                color_continuous_scale='Viridis',
                title="Stock Levels by Store"
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🏷️ Stock by Category")
        if 'category' in curated.columns and 'effective_stock_level' in curated.columns:
            category_stock = curated.groupby('category')['effective_stock_level'].sum().reset_index()
            fig = px.pie(
                category_stock,
                values='effective_stock_level',
                names='category',
                title="Stock Distribution by Category",
                color_discrete_sequence=px.colors.sequential.Viridis
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Stock Movement Analysis
    st.subheader("📊 Stock Movement Analysis")
    if 'product_name' in curated.columns:
        fig = px.scatter(
            curated,
            x='snapshot_level',
            y='effective_stock_level',
            size='incoming_restock',
            color='category' if 'category' in curated.columns else None,
            hover_name='product_name',
            title="Snapshot vs Effective Stock (bubble size = restock quantity)",
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Reconciliation Status
    st.subheader("✅ Reconciliation Status")
    if 'reconciliation_status' in curated.columns:
        status_counts = curated['reconciliation_status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        
        col1, col2 = st.columns([1, 2])
        with col1:
            for _, row in status_counts.iterrows():
                if row['Status'] == 'RECONCILED':
                    st.success(f"✅ {row['Status']}: {row['Count']}")
                else:
                    st.warning(f"⚠️ {row['Status']}: {row['Count']}")
        
        with col2:
            fig = px.bar(
                status_counts,
                x='Status',
                y='Count',
                color='Status',
                color_discrete_map={'RECONCILED': '#00C853', 'FLAGGED_NEGATIVE': '#FFB300'}
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)


def show_run_pipeline():
    """Run pipeline page."""
    st.header("▶️ Run Pipeline")
    
    st.markdown("""
    <div style="background:linear-gradient(145deg,#1E3A5F,#3D5A80); padding:1.5rem; border-radius:10px; margin-bottom:1rem;">
        <h3 style="color:#00D4FF;">🚀 Execute Pipeline</h3>
        <p style="color:white;">Run the complete inventory harmonization pipeline with all validation, reconciliation, and data quality checks.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Pipeline Steps:")
        st.markdown("""
        1. **RAW Layer** - Ingest inventory_snapshot.csv & restock_events.csv
        2. **STAGING Layer** - Validate & Deduplicate
        3. **RECONCILIATION** - Compute effective_stock_level
        4. **CURATED Layer** - Create inventory fact table
        5. **QUARANTINE** - Route invalid records
        """)
    
    with col2:
        st.markdown("### Validation Checks:")
        st.markdown("""
        - ❌ Negative stock detection
        - ❌ Mismatched product_id
        - ❌ Duplicate entries
        - ❌ Restock > logical max
        - ✅ Fuzzy matching for corrections
        """)
    
    st.markdown("---")
    
    if st.button("🚀 Run Pipeline", use_container_width=True):
        with st.spinner("Running pipeline..."):
            # Progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Run pipeline
            status_text.text("Initializing pipeline...")
            progress_bar.progress(10)
            
            try:
                pipeline = InventoryHarmonizationPipeline(config_path="config/task1_config.yml")
                
                status_text.text("Ingesting RAW data...")
                progress_bar.progress(20)
                pipeline.ingest_raw_layer()
                
                status_text.text("Validating and staging data...")
                progress_bar.progress(40)
                pipeline.validate_staging_layer()
                
                status_text.text("Applying fuzzy matching...")
                progress_bar.progress(50)
                pipeline.apply_fuzzy_matching()
                
                status_text.text("Reconciling inventory...")
                progress_bar.progress(60)
                reconciled_df = pipeline.reconcile_inventory()
                
                status_text.text("Creating curated layer...")
                progress_bar.progress(80)
                pipeline.create_curated_layer(reconciled_df)
                
                status_text.text("Saving quarantine data...")
                progress_bar.progress(90)
                pipeline.save_quarantine_layer()
                
                progress_bar.progress(100)
                status_text.text("Pipeline completed successfully!")
                
                # Clear cache to reload data
                st.cache_data.clear()
                
                # Show results
                st.success("✅ Pipeline executed successfully!")
                
                # Display stats
                stats = pipeline.stats
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Inventory Snapshots", stats.snapshot_total)
                with col2:
                    st.metric("Valid Records", stats.snapshot_valid)
                with col3:
                    st.metric("Invalid Records", stats.snapshot_invalid)
                with col4:
                    st.metric("Curated Records", stats.curated_records)
                
                st.markdown("---")
                
                # Validation summary
                st.subheader("Validation Summary")
                validation_data = {
                    'Check': ['Negative Stock', 'Mismatched Product', 'Exceeded Capacity', 'Exceeded Restock Max', 'Duplicates'],
                    'Count': [stats.negative_stock_count, stats.mismatched_product_count, 
                             stats.exceeded_capacity_count, stats.exceeded_restock_max_count, 
                             stats.snapshot_duplicates]
                }
                st.dataframe(pd.DataFrame(validation_data), use_container_width=True)
                
                # Fuzzy matching results
                if stats.fuzzy_matches_found > 0:
                    st.info(f"🔍 Fuzzy Matching: {stats.fuzzy_matches_found} product IDs corrected automatically!")
                
            except Exception as e:
                st.error(f"❌ Pipeline failed: {str(e)}")
                st.exception(e)


def show_data_explorer():
    """Data explorer page."""
    st.header("📊 Data Explorer")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🥇 Curated Data", "🥉 Raw Data", "📦 Products", "🏪 Stores"])
    
    with tab1:
        st.subheader("Curated Inventory Fact Table")
        curated = load_curated_data()
        if curated is not None:
            # Filters
            col1, col2, col3 = st.columns(3)
            with col1:
                if 'store_name' in curated.columns:
                    stores = ['All'] + list(curated['store_name'].unique())
                    selected_store = st.selectbox("Filter by Store", stores)
            with col2:
                if 'category' in curated.columns:
                    categories = ['All'] + list(curated['category'].dropna().unique())
                    selected_category = st.selectbox("Filter by Category", categories)
            with col3:
                if 'reconciliation_status' in curated.columns:
                    statuses = ['All'] + list(curated['reconciliation_status'].unique())
                    selected_status = st.selectbox("Filter by Status", statuses)
            
            # Apply filters
            filtered = curated.copy()
            if 'store_name' in curated.columns and selected_store != 'All':
                filtered = filtered[filtered['store_name'] == selected_store]
            if 'category' in curated.columns and selected_category != 'All':
                filtered = filtered[filtered['category'] == selected_category]
            if 'reconciliation_status' in curated.columns and selected_status != 'All':
                filtered = filtered[filtered['reconciliation_status'] == selected_status]
            
            st.dataframe(filtered, use_container_width=True, height=400)
            
            # Download button
            csv = filtered.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name="inventory_fact.csv",
                mime="text/csv"
            )
        else:
            st.warning("No curated data available. Run the pipeline first!")
    
    with tab2:
        st.subheader("Raw Source Data")
        raw = load_raw_data()
        
        raw_tab1, raw_tab2 = st.tabs(["Inventory Snapshots", "Restock Events"])
        
        with raw_tab1:
            if 'inventory_snapshot' in raw:
                st.dataframe(raw['inventory_snapshot'], use_container_width=True, height=300)
            else:
                st.warning("No inventory snapshot data found")
        
        with raw_tab2:
            if 'restock_events' in raw:
                st.dataframe(raw['restock_events'], use_container_width=True, height=300)
            else:
                st.warning("No restock events data found")
    
    with tab3:
        st.subheader("Products Master")
        raw = load_raw_data()
        if 'products' in raw:
            st.dataframe(raw['products'], use_container_width=True, height=300)
        else:
            st.warning("No products data found")
    
    with tab4:
        st.subheader("Stores Master")
        raw = load_raw_data()
        if 'stores' in raw:
            st.dataframe(raw['stores'], use_container_width=True, height=300)
        else:
            st.warning("No stores data found")


def show_quarantine_audit():
    """Quarantine audit page."""
    st.header("🔍 Quarantine Audit")
    
    st.markdown("""
    <div style="background:linear-gradient(145deg,#B71C1C,#D32F2F); padding:1rem; border-radius:10px; margin-bottom:1rem;">
        <h3 style="color:white;">⚠️ Invalid Records</h3>
        <p style="color:#FFCDD2;">Records that failed validation checks are stored here for investigation and correction.</p>
    </div>
    """, unsafe_allow_html=True)
    
    quarantine = load_quarantine_data()
    
    if not quarantine:
        st.info("No quarantine data found. This means all records passed validation!")
        return
    
    tab1, tab2 = st.tabs(["📦 Inventory Quarantine", "🔄 Restock Quarantine"])
    
    with tab1:
        if 'inventory' in quarantine:
            df = quarantine['inventory']
            st.subheader(f"Invalid Inventory Records ({len(df)})")
            
            # Error type breakdown
            if 'error_type' in df.columns:
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.markdown("### Error Types")
                    error_counts = df['error_type'].value_counts()
                    for error, count in error_counts.items():
                        if 'NEGATIVE' in str(error):
                            st.error(f"❌ {error}: {count}")
                        elif 'MISSING' in str(error):
                            st.warning(f"⚠️ {error}: {count}")
                        else:
                            st.info(f"ℹ️ {error}: {count}")
                
                with col2:
                    fig = px.pie(
                        values=error_counts.values,
                        names=error_counts.index,
                        title="Error Distribution",
                        color_discrete_sequence=px.colors.sequential.Reds
                    )
                    fig.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font_color='white'
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # Fuzzy match suggestions
            if 'suggested_product_id' in df.columns:
                suggestions = df[df['suggested_product_id'].notna()]
                if len(suggestions) > 0:
                    st.subheader("🔍 Fuzzy Match Suggestions")
                    st.success(f"Found {len(suggestions)} potential product ID corrections:")
                    st.dataframe(
                        suggestions[['product_id', 'suggested_product_id', 'fuzzy_match_score']],
                        use_container_width=True
                    )
            
            # Full data
            st.subheader("Full Quarantine Data")
            st.dataframe(df, use_container_width=True, height=300)
        else:
            st.info("No invalid inventory records!")
    
    with tab2:
        if 'restock' in quarantine:
            df = quarantine['restock']
            st.subheader(f"Invalid Restock Records ({len(df)})")
            
            # Error type breakdown
            if 'error_type' in df.columns:
                error_counts = df['error_type'].value_counts()
                fig = px.bar(
                    x=error_counts.index,
                    y=error_counts.values,
                    title="Error Types",
                    color=error_counts.values,
                    color_continuous_scale='Reds'
                )
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='white',
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Full data
            st.dataframe(df, use_container_width=True, height=300)
        else:
            st.info("No invalid restock records!")


def show_configuration():
    """Configuration page."""
    st.header("⚙️ Pipeline Configuration")
    
    config = load_config()
    
    tab1, tab2, tab3, tab4 = st.tabs(["📝 General", "✅ Validation Rules", "🔍 Fuzzy Matching", "📂 Output Paths"])
    
    with tab1:
        st.subheader("Pipeline Information")
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Name:** {config['pipeline']['name']}")
            st.info(f"**Version:** {config['pipeline']['version']}")
        with col2:
            st.info(f"**Description:** {config['pipeline']['description']}")
        
        st.subheader("Data Sources")
        for source, details in config['data_sources'].items():
            with st.expander(f"📁 {source}"):
                st.json(details)
    
    with tab2:
        st.subheader("Validation Rules")
        
        for dataset, rules in config['validation'].items():
            st.markdown(f"### {dataset}")
            
            st.markdown("**Required Fields (Not Null):**")
            for field in rules.get('not_null_fields', []):
                st.markdown(f"- `{field}`")
            
            st.markdown("**Business Rules:**")
            for rule in rules.get('business_rules', []):
                severity_color = "🔴" if rule['severity'] == 'ERROR' else "🟡"
                st.markdown(f"{severity_color} **{rule['name']}**: {rule['description']}")
    
    with tab3:
        st.subheader("Fuzzy Matching Configuration")
        fuzzy_config = config['fuzzy_matching']
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Enabled", "✅ Yes" if fuzzy_config['enabled'] else "❌ No")
            st.metric("Product ID Threshold", f"{fuzzy_config['product_id_matching']['threshold']}%")
        with col2:
            st.metric("Algorithm", fuzzy_config['product_id_matching']['algorithm'])
            st.metric("SKU Pattern", fuzzy_config['sku_validation']['pattern'])
    
    with tab4:
        st.subheader("Output Paths")
        output_config = config['output']
        
        paths = {
            'Raw': output_config['raw']['path'],
            'Staging': output_config['staging']['path'],
            'Curated': output_config['curated']['inventory_fact_table'],
            'Quarantine': output_config['quarantine']['inventory_quarantine']
        }
        
        for layer, path in paths.items():
            st.code(f"{layer}: {path}")


# =============================================================================
# RUN APP
# =============================================================================

if __name__ == "__main__":
    main()

