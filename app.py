import streamlit as st
import pandas as pd
import time
import random
from urllib.parse import quote_plus, urlparse
import json
from io import BytesIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime

@st.cache_resource
def get_driver():
    """Create and cache a Chrome driver for reuse"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    # For Streamlit Cloud deployment
    chrome_options.binary_location = '/usr/bin/chromium'
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        st.error(f"Failed to initialize Chrome driver: {e}")
        st.info("Make sure Chrome/Chromium and chromedriver are installed")
        return None

def is_domain_allowed(url, whitelist=None, blacklist=None):
    """Check if URL domain is allowed based on whitelist/blacklist"""
    if not url:
        return False
    
    try:
        domain = urlparse(url).netloc.lower()
        
        # Check blacklist first (higher priority)
        if blacklist:
            for blocked in blacklist:
                if blocked.lower() in domain:
                    return False
        
        # Check whitelist if provided
        if whitelist:
            return any(allowed.lower() in domain for allowed in whitelist)
        
        return True
    except:
        return False

def search_product_images(description, supplier="", brand="", max_results=2, retry_count=2, 
                         whitelist=None, blacklist=None):
    """
    Search Bing Images for a product with supplier and brand.
    Returns a tuple: (list of image URLs, source page URL from first result)
    """
    image_urls = []
    source_page_url = ""
    
    driver = get_driver()
    if not driver:
        return image_urls, source_page_url
    
    # Build enhanced search query
    search_parts = []
    if brand and brand.strip():
        search_parts.append(brand.strip())
    if supplier and supplier.strip():
        search_parts.append(supplier.strip())
    if description and description.strip():
        search_parts.append(description.strip())
    
    search_parts.append("product")
    search_string = " ".join(search_parts)
    
    for attempt in range(retry_count):
        try:
            search_query = quote_plus(search_string)
            search_url = f"https://www.bing.com/images/search?q={search_query}&first=1"
            
            driver.get(search_url)
            time.sleep(2 + random.uniform(0, 1))
            
            # Find all image containers
            image_containers = driver.find_elements(By.CSS_SELECTOR, 'a.iusc')
            
            for container in image_containers[:max_results * 3]:  # Get more to filter
                if len(image_urls) >= max_results:
                    break
                
                try:
                    m_attr = container.get_attribute('m')
                    if m_attr:
                        data = json.loads(m_attr)
                        
                        # Get source page URL first for filtering
                        page_url = data.get('purl', '')
                        
                        # Check if domain is allowed
                        if not is_domain_allowed(page_url, whitelist, blacklist):
                            continue
                        
                        # Get image URL
                        image_url = data.get('murl') or data.get('turl')
                        
                        if image_url and len(image_url) > 20:
                            image_urls.append(image_url)
                            
                            if not source_page_url and page_url:
                                source_page_url = page_url
                            
                except Exception:
                    continue
            
            if image_urls:
                break
                
        except Exception as e:
            if attempt < retry_count - 1:
                time.sleep(3)
                continue
            else:
                st.warning(f"Search failed after {retry_count} attempts: {str(e)}")
    
    return image_urls[:max_results], source_page_url

def create_hyperlink_formula(url, display_text="View"):
    """Create Excel hyperlink formula"""
    if not url or url == "":
        return ""
    # Excel HYPERLINK formula
    return f'=HYPERLINK("{url}","{display_text}")'

def process_dataframe(df, product_id_col, description_col, supplier_col, brand_col, 
                     num_rows, whitelist=None, blacklist=None):
    """
    Process dataframe and add image URL columns with progress tracking.
    Returns tuple: (results_df, stats_df)
    """
    # Initialize or get session state for checkpointing
    if 'checkpoint_data' not in st.session_state:
        st.session_state.checkpoint_data = None
    if 'last_processed_index' not in st.session_state:
        st.session_state.last_processed_index = -1
    
    # Limit rows to process
    df_to_process = df.head(num_rows).copy()
    
    # Create results dataframe with ONLY selected columns
    selected_cols = [product_id_col, description_col]
    
    # Handle supplier column
    use_supplier_col = supplier_col and supplier_col != '_temp_supplier'
    if use_supplier_col:
        selected_cols.append(supplier_col)
    else:
        df_to_process['_temp_supplier'] = ""
        supplier_col = '_temp_supplier'
    
    # Handle brand column
    use_brand_col = brand_col and brand_col != '_temp_brand'
    if use_brand_col:
        selected_cols.append(brand_col)
    else:
        df_to_process['_temp_brand'] = ""
        brand_col = '_temp_brand'
    
    # Now create results df with all needed columns
    results_df = df_to_process[selected_cols].copy()
    
    # Add new columns for results (clean output)
    results_df['Image_URL_1'] = ''
    results_df['Image_URL_2'] = ''
    results_df['Product_Page_URL'] = ''
    
    # Create separate stats dataframe for tracking
    stats_df = pd.DataFrame()
    stats_df['Product_ID'] = df_to_process[product_id_col]
    stats_df['Search_Query'] = ''
    stats_df['Source_Domain'] = ''
    stats_df['Images_Found'] = 0
    stats_df['Search_Status'] = ''
    stats_df['Processed_DateTime'] = ''
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    results_container = st.container()
    
    total_rows = len(results_df)
    success_count = 0
    failed_count = 0
    filtered_count = 0
    checkpoint_interval = 10
    
    # Metrics display
    col1, col2, col3, col4 = st.columns(4)
    metric_processed = col1.empty()
    metric_success = col2.empty()
    metric_failed = col3.empty()
    metric_filtered = col4.empty()
    
    for idx, row in results_df.iterrows():
        # Update progress
        current_row = idx + 1
        progress = current_row / total_rows
        progress_bar.progress(progress)
        
        # Get product details
        product_id = str(row[product_id_col]) if pd.notna(row[product_id_col]) else f"Row_{idx}"
        description = str(row[description_col]) if pd.notna(row[description_col]) else ""
        
        # Get supplier and brand from df_to_process (which has the temp columns)
        supplier = str(df_to_process.loc[idx, supplier_col]) if pd.notna(df_to_process.loc[idx, supplier_col]) else ""
        brand = str(df_to_process.loc[idx, brand_col]) if pd.notna(df_to_process.loc[idx, brand_col]) else ""
        
        status_text.text(f"Processing {current_row}/{total_rows} | Product ID: {product_id} | {description[:40]}...")
        
        # Build search query for display
        search_query_parts = [p for p in [brand, supplier, description] if p.strip()]
        search_query = " ".join(search_query_parts)
        
        # Search for images
        try:
            image_urls, product_url = search_product_images(
                description=description,
                supplier=supplier,
                brand=brand,
                max_results=2,
                retry_count=2,
                whitelist=whitelist,
                blacklist=blacklist
            )
            
            # Add URLs to dataframe (plain text, no hyperlinks)
            for i, url in enumerate(image_urls, 1):
                results_df.at[idx, f'Image_URL_{i}'] = url
            
            if product_url:
                results_df.at[idx, 'Product_Page_URL'] = product_url
            
            # Extract domain for stats
            source_domain = ""
            if product_url:
                try:
                    source_domain = urlparse(product_url).netloc
                except:
                    pass
            
            # Update stats dataframe
            stats_df.at[idx, 'Search_Query'] = search_query
            stats_df.at[idx, 'Source_Domain'] = source_domain
            stats_df.at[idx, 'Images_Found'] = len(image_urls)
            stats_df.at[idx, 'Processed_DateTime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Display progress
            with results_container:
                if image_urls:
                    success_count += 1
                    stats_df.at[idx, 'Search_Status'] = 'Success'
                    domain_info = f" from {source_domain}" if source_domain else ""
                    st.success(f"✓ [{product_id}] Found {len(image_urls)} image(s){domain_info} - {description[:50]}...")
                else:
                    if whitelist or blacklist:
                        filtered_count += 1
                        stats_df.at[idx, 'Search_Status'] = 'Filtered - No allowed domains'
                        st.info(f"🔍 [{product_id}] No images from allowed domains - {description[:50]}...")
                    else:
                        failed_count += 1
                        stats_df.at[idx, 'Search_Status'] = 'Failed - No images found'
                        st.warning(f"⚠ [{product_id}] No images found - {description[:50]}...")
            
        except Exception as e:
            failed_count += 1
            stats_df.at[idx, 'Search_Status'] = f'Error: {str(e)[:50]}'
            with results_container:
                st.error(f"✗ [{product_id}] Error: {str(e)[:100]}")
        
        # Update metrics
        metric_processed.metric("Processed", f"{current_row}/{total_rows}")
        metric_success.metric("Success", success_count)
        metric_failed.metric("Failed", failed_count)
        if whitelist or blacklist:
            metric_filtered.metric("Filtered", filtered_count)
        
        # Checkpoint every N items
        if current_row % checkpoint_interval == 0:
            st.session_state.checkpoint_data = results_df.copy()
            st.session_state.last_processed_index = idx
            status_text.text(f"💾 Checkpoint saved at row {current_row}")
            time.sleep(0.5)
        
        # Adaptive delay to avoid rate limiting
        base_delay = 2.5
        if failed_count > success_count * 0.3:  # If >30% failure rate
            delay = random.uniform(base_delay + 1, base_delay + 3)
        else:
            delay = random.uniform(base_delay, base_delay + 1.5)
        
        time.sleep(delay)
    
    progress_bar.progress(1.0)
    success_rate = (success_count / total_rows * 100) if total_rows > 0 else 0
    status_text.text(f"✓ Complete! {total_rows} processed | {success_count} successful ({success_rate:.1f}%) | {failed_count} failed")
    
    return results_df, stats_df

def create_excel_output(results_df, stats_df):
    """
    Create Excel file with Results sheet and Statistics sheet.
    """
    output = BytesIO()
    
    # Calculate summary statistics
    total_processed = len(stats_df)
    total_success = (stats_df['Search_Status'] == 'Success').sum()
    total_failed = stats_df['Search_Status'].str.contains('Failed', na=False).sum()
    total_filtered = stats_df['Search_Status'].str.contains('Filtered', na=False).sum()
    total_errors = stats_df['Search_Status'].str.contains('Error', na=False).sum()
    
    success_rate = (total_success / total_processed * 100) if total_processed > 0 else 0
    
    # Get top domains
    domain_counts = stats_df[stats_df['Source_Domain'] != '']['Source_Domain'].value_counts()
    top_3_domains = domain_counts.head(3)
    
    # Create summary dataframe
    summary_data = {
        'Metric': [
            'Total Products Processed',
            'Successful Searches',
            'Failed Searches',
            'Filtered (Domain Rules)',
            'Errors',
            'Success Rate (%)',
            '',
            'TOP 3 SOURCE DOMAINS',
        ],
        'Value': [
            total_processed,
            total_success,
            total_failed,
            total_filtered,
            total_errors,
            f"{success_rate:.1f}%",
            '',
            '',
        ]
    }
    
    # Add top 3 domains to summary
    for i, (domain, count) in enumerate(top_3_domains.items(), 1):
        summary_data['Metric'].append(f"#{i} - {domain}")
        summary_data['Value'].append(f"{count} products ({count/total_processed*100:.1f}%)")
    
    # Add notable findings
    summary_data['Metric'].extend(['', 'NOTABLE FINDINGS'])
    summary_data['Value'].extend(['', ''])
    
    # Check for patterns
    if total_filtered > total_processed * 0.3:
        summary_data['Metric'].append('⚠ High Filter Rate')
        summary_data['Value'].append(f'{total_filtered} products filtered - consider relaxing domain rules')
    
    if total_failed > total_processed * 0.2:
        summary_data['Metric'].append('⚠ High Failure Rate')
        summary_data['Value'].append(f'{total_failed} products failed - check search query quality')
    
    unique_domains = stats_df['Source_Domain'].nunique()
    summary_data['Metric'].append('Source Diversity')
    summary_data['Value'].append(f'{unique_domains} unique domains found')
    
    if unique_domains < 5 and total_success > 10:
        summary_data['Metric'].append('⚠ Low Domain Diversity')
        summary_data['Value'].append('Results concentrated in few sources - may indicate bias')
    
    summary_df = pd.DataFrame(summary_data)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Write Results sheet (clean)
        results_df.to_excel(writer, sheet_name='Results', index=False)
        
        # Write Summary sheet
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Write detailed Statistics sheet
        stats_df.to_excel(writer, sheet_name='Statistics', index=False)
        
        # Auto-adjust column widths for all sheets
        for sheet_name in ['Results', 'Summary', 'Statistics']:
            worksheet = writer.sheets[sheet_name]
            df_to_check = results_df if sheet_name == 'Results' else (summary_df if sheet_name == 'Summary' else stats_df)
            for idx, col in enumerate(df_to_check.columns):
                max_length = max(
                    df_to_check[col].astype(str).apply(len).max(),
                    len(col)
                )
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 60)
    
    output.seek(0)
    return output

# Streamlit App
st.set_page_config(page_title="Product Image URL Scraper v3", page_icon="🖼️", layout="wide")

st.title("🖼️ Product Image URL Scraper v3")
st.markdown("Upload an Excel file to scrape product images from Bing Images with domain filtering")

# File upload
uploaded_file = st.file_uploader("Upload Excel File", type=['xlsx', 'xls'])

if uploaded_file:
    try:
        # Read Excel file
        excel_file = pd.ExcelFile(uploaded_file)
        sheet_names = excel_file.sheet_names
        
        # Sheet selection
        st.subheader("📋 Step 1: Select Sheet")
        selected_sheet = st.selectbox("Choose the sheet to process:", sheet_names)
        
        # Read selected sheet
        df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
        
        st.success(f"✓ Loaded sheet '{selected_sheet}' with {len(df)} rows and {len(df.columns)} columns")
        
        # Column selection
        st.subheader("📝 Step 2: Select Columns")
        
        col1, col2 = st.columns(2)
        with col1:
            product_id_column = st.selectbox(
                "Product ID Column:",
                df.columns.tolist(),
                help="Unique identifier for each product"
            )
        with col2:
            description_column = st.selectbox(
                "Product Description Column:",
                df.columns.tolist(),
                help="Main product description for search"
            )
        
        col3, col4 = st.columns(2)
        with col3:
            supplier_column = st.selectbox(
                "Supplier Column:",
                ["(None)"] + df.columns.tolist(),
                help="Optional: Supplier name to improve search"
            )
        with col4:
            brand_column = st.selectbox(
                "Brand Column:",
                ["(None)"] + df.columns.tolist(),
                help="Optional: Brand name to improve search"
            )
        
        # Domain filtering options
        st.subheader("🔍 Step 3: Domain Filtering (Optional)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            use_whitelist = st.checkbox("Enable Whitelist (Only allowed domains)", value=False)
            if use_whitelist:
                whitelist_input = st.text_area(
                    "Allowed Domains (one per line)",
                    value="amazon.com\nmanufacturer-site.com\nofficial-store.com",
                    help="Only scrape from these domains"
                )
                whitelist = [d.strip() for d in whitelist_input.split('\n') if d.strip()]
            else:
                whitelist = None
        
        with col2:
            use_blacklist = st.checkbox("Enable Blacklist (Block domains)", value=True)
            if use_blacklist:
                blacklist_input = st.text_area(
                    "Blocked Domains (one per line)",
                    value="pinterest.com\nfacebook.com\ninstagram.com\naliexpress.com\ntemu.com\nwish.com",
                    help="Never scrape from these domains"
                )
                blacklist = [d.strip() for d in blacklist_input.split('\n') if d.strip()]
            else:
                blacklist = None
        
        # Display active filters
        if whitelist or blacklist:
            st.info(f"🔒 Active filters: {len(whitelist) if whitelist else 0} allowed domains, {len(blacklist) if blacklist else 0} blocked domains")
        
        # Preview selected columns
        with st.expander("Preview first 5 products"):
            preview_cols = [product_id_column, description_column]
            if supplier_column != "(None)":
                preview_cols.append(supplier_column)
            if brand_column != "(None)":
                preview_cols.append(brand_column)
            st.dataframe(df[preview_cols].head())
        
        # Number of rows to process
        st.subheader("🔢 Step 4: Set Number of Rows to Process")
        max_rows = len(df)
        num_rows = st.number_input(
            f"How many rows to process? (Max: {max_rows}, Recommended: 100)",
            min_value=1,
            max_value=max_rows,
            value=min(100, max_rows),
            step=10
        )
        
        # Estimate time
        estimated_time = num_rows * 3.5 / 60  # ~3.5 seconds per item
        st.info(f"📊 Processing {num_rows} rows | Estimated time: ~{estimated_time:.1f} minutes")
        
        # Process button
        st.subheader("🚀 Step 5: Start Processing")
        
        if st.button("🔍 Start Scraping", type="primary"):
            st.markdown("---")
            st.subheader("⏳ Processing...")
            
            start_time = time.time()
            
            # Process the dataframe
            supplier_col = supplier_column if supplier_column != "(None)" else None
            brand_col = brand_column if brand_column != "(None)" else None
            
            # Pass None for columns not selected, function will handle temp columns internally
            results_df, stats_df = process_dataframe(
                df, 
                product_id_column, 
                description_column, 
                supplier_col,
                brand_col,
                num_rows,
                whitelist=whitelist,
                blacklist=blacklist
            )
            
            elapsed_time = (time.time() - start_time) / 60
            
            st.markdown("---")
            st.success(f"✅ Processing Complete in {elapsed_time:.1f} minutes!")
            
            # Display results preview
            st.subheader("📊 Results Preview")
            st.dataframe(results_df.head(10))
            
            # Create download file
            output_file = create_excel_output(results_df, stats_df)
            
            # Download button
            st.subheader("💾 Download Results")
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            st.download_button(
                label="📥 Download Excel with Results & Statistics",
                data=output_file,
                file_name=f"Product_URLs_Results_{num_rows}rows_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            st.info("📋 **Excel file contains 3 sheets:**\n- **Results**: Clean data with only selected columns + URLs\n- **Summary**: Overview with top domains and notable findings\n- **Statistics**: Detailed tracking data for research")
    
    except Exception as e:
        st.error(f"Error: {str(e)}")
        import traceback
        with st.expander("Show full error"):
            st.code(traceback.format_exc())

else:
    st.info("👆 Please upload an Excel file to get started")
    
    # Instructions
    with st.expander("ℹ️ How to use this app"):
        st.markdown("""
        ### Instructions:
        1. **Upload** your Excel file containing product data
        2. **Select** the sheet you want to process
        3. **Choose** the required columns:
           - **Product ID**: Unique identifier for each product
           - **Description**: Main product description
           - **Supplier** (optional): Improves search accuracy
           - **Brand** (optional): Improves search accuracy
        4. **Configure domain filtering** (optional but recommended):
           - **Whitelist**: Only scrape from trusted domains
           - **Blacklist**: Exclude low-quality sources
        5. **Set** how many rows to process
        6. **Click** "Start Scraping" and wait for completion
        7. **Download** the Excel file with 3 sheets
        
        ### Output File Structure:
        - 📄 **Results Sheet**: Clean data ready for work
          - Your selected columns + Image URLs + Product Page URL
        - 📊 **Summary Sheet**: Quick overview
          - Success metrics, top 3 domains, notable findings
        - 📈 **Statistics Sheet**: Detailed tracking
          - Product ID, search queries, domains, timestamps, status
        
        ### What's New in v3:
        - ✅ **Clean Results sheet** - only columns you need
        - ✅ **Separate Statistics sheet** - for analysis and improvement
        - ✅ **Summary sheet** - instant insights and recommendations
        - ✅ **Plain text URLs** - easy to copy and use
        - ✅ **Domain filtering** with whitelist/blacklist
        - ✅ **Automatic insights** - highlights issues and patterns
        
        ### Domain Filtering Tips:
        - **Whitelist**: Use for official sources only (manufacturer sites, major retailers)
        - **Blacklist**: Block Pinterest, social media, dropshipping sites
        - **Recommended blacklist**: pinterest, facebook, instagram, aliexpress, temu, wish
        
        ### Performance:
        - **100 items**: ~6-7 minutes
        - **Processing**: 3-4 seconds per product
        - **Success rate**: Typically 85-95% (may be lower with strict filtering)
        """)
    
    with st.expander("⚙️ Recommended Domain Settings"):
        st.markdown("""
        ### Suggested Whitelists by Industry:
        
        **Electronics:**
        ```
        amazon.com
        bestbuy.com
        newegg.com
        bhphotovideo.com
        manufacturer websites
        ```
        
        **Fashion/Apparel:**
        ```
        nordstrom.com
        macys.com
        zappos.com
        brand official sites
        ```
        
        **Industrial/B2B:**
        ```
        grainger.com
        mcmaster.com
        mscdirect.com
        supplier websites
        ```
        
        ### Always Blacklist:
        ```
        pinterest.com
        facebook.com
        instagram.com
        aliexpress.com
        temu.com
        wish.com
        ebay.com (optional)
        etsy.com (optional)
        ```
        """)
      