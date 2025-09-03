#!/usr/bin/env python3
"""
Comprehensive Email Enrichment Pipeline
Processes all CSV files in data/ directory through the complete pipeline:
1. Clean existing email columns
2. Enrich LinkedIn profiles with emails using Clado API
3. Filter people with emails
4. Apply corporate email filtering
5. Consolidate to one email per person (prioritizing personal > work > edu)
6. Generate analysis reports
"""

import pandas as pd
import aiohttp
import asyncio
import os
import glob
import sys
import time
import json
import csv
import pickle
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import Counter

# Clado API configuration
API_BASE_URL = "https://search.clado.ai/api/enrich"
API_KEY = "lk_4b5dc217320e403b91ca1f28c4307921"

# Bulk API settings
MAX_URLS_PER_BATCH = 500
POLL_INTERVAL = 5
MAX_POLL_TIME = 3600

# Personal email domains
PERSONAL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 
    'aol.com', 'icloud.com', 'me.com', 'mac.com', 
    'msn.com', 'live.com', 'yahoo.co.uk', 'yahoo.ca',
    'yahoo.fr', 'yahoo.de', 'yahoo.es', 'yahoo.it',
    'googlemail.com', 'ymail.com', 'rocketmail.com',
    'protonmail.com', 'proton.me', 'pm.me',
    'mail.com', 'email.com', 'usa.com',
    'fastmail.com', 'fastmail.fm', 'zoho.com',
    'qq.com', '163.com', '126.com', 'sina.com',
    'gmx.com', 'gmx.net', 'web.de',
    'tutanota.com', 'tutanota.de', 'tuta.io',
    'comcast.net', 'verizon.net', 'att.net',
    'sbcglobal.net', 'bellsouth.net', 'cox.net'
}

# Academic domains
ACADEMIC_EXTENSIONS = {'.edu', '.ac.uk', '.ac.jp', '.ac.cn', '.ac.in', '.edu.au', '.edu.cn'}

def is_personal_email(email):
    """Check if an email is from a personal domain."""
    if not email or '@' not in email:
        return False
    
    email = email.strip().lower()
    domain = email.split('@')[1] if '@' in email else ''
    
    if domain in PERSONAL_DOMAINS:
        return True
    
    if any(domain.endswith(ext) for ext in ACADEMIC_EXTENSIONS):
        return True
    
    if any(term in domain for term in ['university', 'college', 'institute', 'academia']):
        return True
    
    return False

def is_edu_email(email):
    """Check if an email is from an educational domain."""
    if not email or '@' not in email:
        return False
    
    email = email.strip().lower()
    domain = email.split('@')[1] if '@' in email else ''
    
    if any(domain.endswith(ext) for ext in ACADEMIC_EXTENSIONS):
        return True
    
    if any(term in domain for term in ['university', 'college', 'institute', 'academia']):
        return True
    
    return False

def prioritize_email(email):
    """
    Prioritize emails: personal (1) > work (2) > edu (3)
    Returns lower number for higher priority
    """
    if pd.isna(email) or not email:
        return 999
    
    if is_personal_email(email) and not is_edu_email(email):
        return 1  # Personal (highest priority)
    elif is_edu_email(email):
        return 3  # Educational (lowest priority)
    else:
        return 2  # Work/Corporate (medium priority)

async def submit_bulk_job(session: aiohttp.ClientSession, 
                         linkedin_urls: List[str],
                         email_enrichment: bool = True,
                         phone_enrichment: bool = False) -> Optional[str]:
    """Submit a bulk enrichment job to Clado API."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "linkedin_urls": linkedin_urls,
        "email_enrichment": email_enrichment,
        "phone_enrichment": phone_enrichment
    }
    
    try:
        async with session.post(f"{API_BASE_URL}/bulk-contacts", 
                               headers=headers, 
                               json=payload) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('job_id')
            elif response.status == 401:
                text = await response.text()
                print(f"Authentication error: {text}")
                return None
            elif response.status == 402:
                text = await response.text()
                print(f"Insufficient credits: {text}")
                return None
            else:
                text = await response.text()
                print(f"Error submitting bulk job: {response.status} - {text}")
                return None
    except Exception as e:
        print(f"Exception submitting bulk job: {str(e)}")
        return None

async def poll_job_status(session: aiohttp.ClientSession, 
                         job_id: str) -> Optional[Dict]:
    """Poll the status of a bulk enrichment job."""
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    
    try:
        async with session.get(f"{API_BASE_URL}/bulk-contacts/{job_id}", 
                              headers=headers) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 404:
                print(f"Job {job_id} not found or expired")
                return None
            else:
                text = await response.text()
                print(f"Error checking job status: {response.status} - {text}")
                return None
    except Exception as e:
        print(f"Exception checking job status: {str(e)}")
        return None

async def wait_for_job_completion(session: aiohttp.ClientSession, 
                                 job_id: str,
                                 total_urls: int) -> Optional[Dict]:
    """Wait for a bulk enrichment job to complete."""
    start_time = time.time()
    last_processed = 0
    milestone_count = 50
    next_milestone = milestone_count
    
    print(f"\n⏳ Job {job_id} submitted. Polling for results...")
    print(f"Will report progress every {milestone_count} profiles completed...\n")
    
    while True:
        if time.time() - start_time > MAX_POLL_TIME:
            print(f"\n❌ Job polling timeout after {MAX_POLL_TIME} seconds")
            return None
        
        status_data = await poll_job_status(session, job_id)
        
        if not status_data:
            return None
        
        status = status_data.get('status', 'unknown')
        processed = status_data.get('processed', 0)
        successful = status_data.get('successful', 0)
        failed = status_data.get('failed', 0)
        
        if processed > last_processed:
            if processed >= next_milestone or processed == total_urls:
                percentage = (processed / total_urls) * 100
                current_time = datetime.now().strftime("%H:%M:%S")
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                print(f"[{current_time}] ✓ {processed}/{total_urls} profiles completed ({percentage:.1f}%) - {rate:.1f} profiles/sec")
                print(f"            ✅ Successful: {successful} | ❌ Failed: {failed}")
                next_milestone += milestone_count
            last_processed = processed
        
        if status in ['completed', 'error']:
            if status == 'completed':
                print(f"\n✅ Job {job_id} completed successfully!")
            else:
                print(f"\n❌ Job {job_id} failed with error")
            return status_data
        
        await asyncio.sleep(POLL_INTERVAL)

def parse_bulk_result(result: Dict) -> Dict[str, str]:
    """Parse a single result from bulk enrichment."""
    parsed = {}
    
    if not result.get('success'):
        parsed['Enrichment Status'] = f"Error: {result.get('error', 'Unknown error')}"
        return parsed
    
    data = result.get('data', {})
    
    contacts = data.get('contacts', [])
    work_emails = []
    personal_emails = []
    phones = []
    
    for contact in contacts:
        contact_type = contact.get('type')
        value = contact.get('value')
        sub_type = contact.get('subType')
        
        if contact_type == 'email':
            if sub_type == 'work':
                work_emails.append(value)
            else:
                personal_emails.append(value)
        elif contact_type == 'phone':
            phones.append(value)
    
    parsed['Work Email (Clado)'] = '; '.join(work_emails) if work_emails else ''
    parsed['Personal Email (Clado)'] = '; '.join(personal_emails) if personal_emails else ''
    parsed['Phone (Clado)'] = '; '.join(phones) if phones else ''
    
    if contacts:
        parsed['Enrichment Status'] = 'Success'
    else:
        parsed['Enrichment Status'] = 'No contacts found'
    
    return parsed

def clean_existing_email_columns(df):
    """Remove existing email columns that might interfere."""
    email_columns_to_remove = [
        "Find work email", "Find Work Email", "Find Work Email (2)",
        "Find work email (2)", "Find work email (3)", "Find Work Email (3)",
        "Find email", "Work Email"
    ]
    
    columns_to_drop = [col for col in email_columns_to_remove if col in df.columns]
    
    if columns_to_drop:
        print(f"  Removing existing email columns: {columns_to_drop}")
        df = df.drop(columns=columns_to_drop)
    
    return df

def save_checkpoint(dataset_name, step, df, extra_data=None):
    """Save checkpoint data."""
    checkpoint_dir = "checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    checkpoint_file = f"{checkpoint_dir}/{dataset_name}_step_{step}.pkl"
    checkpoint_data = {
        'dataset_name': dataset_name,
        'step': step,
        'timestamp': datetime.now().isoformat(),
        'dataframe': df,
        'extra_data': extra_data or {}
    }
    
    with open(checkpoint_file, 'wb') as f:
        pickle.dump(checkpoint_data, f)
    
    print(f"  💾 Checkpoint saved: {checkpoint_file}")

def load_checkpoint(dataset_name, step):
    """Load checkpoint data."""
    checkpoint_dir = "checkpoints"
    checkpoint_file = f"{checkpoint_dir}/{dataset_name}_step_{step}.pkl"
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'rb') as f:
            checkpoint_data = pickle.load(f)
        print(f"  📂 Loaded checkpoint: {checkpoint_file}")
        return checkpoint_data
    
    return None

def get_latest_checkpoint(dataset_name):
    """Find the latest checkpoint for a dataset."""
    checkpoint_dir = "checkpoints"
    if not os.path.exists(checkpoint_dir):
        return None
    
    checkpoint_files = glob.glob(f"{checkpoint_dir}/{dataset_name}_step_*.pkl")
    if not checkpoint_files:
        return None
    
    # Extract step numbers and find the highest
    step_numbers = []
    for file in checkpoint_files:
        try:
            step = int(file.split('_step_')[1].split('.pkl')[0])
            step_numbers.append(step)
        except:
            continue
    
    if step_numbers:
        latest_step = max(step_numbers)
        return load_checkpoint(dataset_name, latest_step)
    
    return None

async def enrich_with_clado(df, linkedin_column, dataset_name):
    """Enrich DataFrame with Clado API with checkpointing."""
    print(f"  Starting Clado enrichment for {len(df)} profiles...")
    
    # Check for existing enrichment checkpoint
    checkpoint = load_checkpoint(dataset_name, 3)
    if checkpoint:
        print(f"  ✅ Found enrichment checkpoint, skipping API calls")
        return checkpoint['dataframe']
    
    # Initialize new columns
    new_columns = [
        'Enrichment Status',
        'Work Email (Clado)',
        'Personal Email (Clado)',
        'Phone (Clado)'
    ]
    
    for col in new_columns:
        df[col] = ''
    
    # Get valid LinkedIn URLs
    valid_urls = []
    url_to_index = {}
    
    for idx, row in df.iterrows():
        linkedin_url = row.get(linkedin_column)
        if pd.notna(linkedin_url) and linkedin_url:
            valid_urls.append(linkedin_url)
            url_to_index[linkedin_url] = idx
        else:
            df.loc[idx, 'Enrichment Status'] = 'No LinkedIn URL'
    
    if not valid_urls:
        print("  No valid LinkedIn URLs found")
        return df
    
    print(f"  Found {len(valid_urls)} profiles with LinkedIn URLs")
    
    async with aiohttp.ClientSession() as session:
        # Process in batches with checkpointing
        batch_number = 1
        for batch_start in range(0, len(valid_urls), MAX_URLS_PER_BATCH):
            batch_end = min(batch_start + MAX_URLS_PER_BATCH, len(valid_urls))
            batch_urls = valid_urls[batch_start:batch_end]
            
            print(f"  Processing batch {batch_number}: {len(batch_urls)} URLs")
            
            try:
                # Submit bulk job
                job_id = await submit_bulk_job(session, batch_urls)
                
                if not job_id:
                    print(f"  ❌ Failed to submit batch {batch_number}")
                    for url in batch_urls:
                        if url in url_to_index:
                            idx = url_to_index[url]
                            df.loc[idx, 'Enrichment Status'] = 'Error: Batch submission failed'
                    batch_number += 1
                    continue
                
                # Wait for job completion
                job_results = await wait_for_job_completion(session, job_id, len(batch_urls))
                
                if job_results:
                    batch_results = job_results.get('results', [])
                    
                    for result in batch_results:
                        linkedin_url = result.get('linkedin_url')
                        if linkedin_url and linkedin_url in url_to_index:
                            idx = url_to_index[linkedin_url]
                            parsed_data = parse_bulk_result(result)
                            for col, value in parsed_data.items():
                                if col in df.columns:
                                    df.loc[idx, col] = value
                    
                    print(f"  ✅ Batch {batch_number} completed successfully")
                    
                    # Save intermediate checkpoint every 2 batches
                    if batch_number % 2 == 0:
                        save_checkpoint(dataset_name, f"3_batch_{batch_number}", df)
                        
                else:
                    print(f"  ❌ Batch {batch_number} failed")
                    for url in batch_urls:
                        if url in url_to_index:
                            idx = url_to_index[url]
                            df.loc[idx, 'Enrichment Status'] = 'Error: Job failed'
                            
            except Exception as e:
                print(f"  ❌ Error in batch {batch_number}: {e}")
                for url in batch_urls:
                    if url in url_to_index:
                        idx = url_to_index[url]
                        df.loc[idx, 'Enrichment Status'] = f'Error: {str(e)}'
            
            batch_number += 1
    
    # Save final enrichment checkpoint
    save_checkpoint(dataset_name, 3, df)
    
    return df

def filter_people_with_emails(df):
    """Filter to keep only people with email addresses."""
    work_email_col = 'Work Email (Clado)'
    personal_email_col = 'Personal Email (Clado)'
    
    has_work_email = df[work_email_col].notna() & (df[work_email_col] != '')
    has_personal_email = df[personal_email_col].notna() & (df[personal_email_col] != '')
    has_any_email = has_work_email | has_personal_email
    
    before_count = len(df)
    df_filtered = df[has_any_email].copy()
    after_count = len(df_filtered)
    
    print(f"  Filtered from {before_count} to {after_count} people with emails")
    
    return df_filtered

def apply_corporate_email_filtering(df):
    """Apply corporate email filtering logic."""
    work_email_col = 'Work Email (Clado)'
    personal_email_col = 'Personal Email (Clado)'
    
    def process_email_field(email_field):
        if pd.isna(email_field) or not email_field:
            return [], []
        
        emails = str(email_field).split(';')
        personal_emails = []
        corporate_emails = []
        
        for email in emails:
            email = email.strip()
            if email and '@' in email:
                if is_personal_email(email):
                    personal_emails.append(email)
                else:
                    corporate_emails.append(email)
        
        return personal_emails, corporate_emails
    
    people_with_only_corporate = 0
    
    # Reset index to ensure continuous indexing
    df = df.reset_index(drop=True)
    
    for idx in range(len(df)):
        work_emails_str = df.iloc[idx][work_email_col] if work_email_col in df.columns else ''
        personal_emails_str = df.iloc[idx][personal_email_col] if personal_email_col in df.columns else ''
        
        work_personal, work_corporate = process_email_field(work_emails_str)
        personal_personal, personal_corporate = process_email_field(personal_emails_str)
        
        all_personal = work_personal + personal_personal
        all_corporate = work_corporate + personal_corporate
        
        if all_personal:
            # Person has personal emails, use only those
            df.iloc[idx, df.columns.get_loc(work_email_col)] = '; '.join(work_personal) if work_personal else ''
            df.iloc[idx, df.columns.get_loc(personal_email_col)] = '; '.join(personal_personal) if personal_personal else ''
        elif all_corporate:
            # Person only has corporate emails, keep them
            people_with_only_corporate += 1
    
    print(f"  {people_with_only_corporate} people have only corporate emails (kept)")
    
    return df

def consolidate_to_one_email_per_person(df):
    """Consolidate to one email per person, prioritizing personal > work > edu."""
    work_email_col = 'Work Email (Clado)'
    personal_email_col = 'Personal Email (Clado)'
    
    def get_all_emails_from_row(row):
        all_emails = []
        
        # Get work emails
        if pd.notna(row.get(work_email_col)) and row.get(work_email_col):
            emails = str(row[work_email_col]).split(';')
            all_emails.extend([e.strip() for e in emails if e.strip() and '@' in e])
        
        # Get personal emails
        if pd.notna(row.get(personal_email_col)) and row.get(personal_email_col):
            emails = str(row[personal_email_col]).split(';')
            all_emails.extend([e.strip() for e in emails if e.strip() and '@' in e])
        
        return all_emails
    
    def select_best_email(emails):
        if not emails:
            return None
        
        # Sort by priority (personal=1, work=2, edu=3)
        emails_with_priority = [(prioritize_email(email), email) for email in emails]
        emails_with_priority.sort(key=lambda x: x[0])
        
        return emails_with_priority[0][1]
    
    # Create consolidated email column
    df['Consolidated_Email'] = df.apply(
        lambda row: select_best_email(get_all_emails_from_row(row)), 
        axis=1
    )
    
    # Count email types
    personal_count = sum(1 for email in df['Consolidated_Email'] if prioritize_email(email) == 1)
    work_count = sum(1 for email in df['Consolidated_Email'] if prioritize_email(email) == 2)
    edu_count = sum(1 for email in df['Consolidated_Email'] if prioritize_email(email) == 3)
    
    print(f"  Consolidated emails - Personal: {personal_count}, Work: {work_count}, Educational: {edu_count}")
    
    return df

def analyze_final_results(df, dataset_name):
    """Generate analysis report for the final dataset."""
    print(f"\n📊 ANALYSIS REPORT - {dataset_name.upper()}")
    print(f"{'='*60}")
    
    # Basic stats
    total_people = len(df)
    people_with_emails = len(df[df['Consolidated_Email'].notna()])
    
    print(f"Total people: {total_people}")
    print(f"People with emails: {people_with_emails} ({people_with_emails/total_people*100:.1f}%)")
    
    # Email type breakdown
    if 'Consolidated_Email' in df.columns:
        personal_count = sum(1 for email in df['Consolidated_Email'] if prioritize_email(email) == 1)
        work_count = sum(1 for email in df['Consolidated_Email'] if prioritize_email(email) == 2)
        edu_count = sum(1 for email in df['Consolidated_Email'] if prioritize_email(email) == 3)
        
        print(f"\nEmail Type Distribution:")
        print(f"  Personal emails: {personal_count} ({personal_count/people_with_emails*100:.1f}%)")
        print(f"  Work emails: {work_count} ({work_count/people_with_emails*100:.1f}%)")
        print(f"  Educational emails: {edu_count} ({edu_count/people_with_emails*100:.1f}%)")
    
    # Top domains
    if 'Consolidated_Email' in df.columns:
        valid_emails = df[df['Consolidated_Email'].notna()]['Consolidated_Email']
        domains = [email.split('@')[1].lower() for email in valid_emails if '@' in str(email)]
        domain_counts = Counter(domains)
        
        print(f"\nTop 10 Email Domains:")
        for domain, count in domain_counts.most_common(10):
            print(f"  {domain:25} {count:4} emails ({count/len(domains)*100:.1f}%)")

async def process_single_dataset(file_path):
    """Process a single dataset through the complete pipeline with checkpointing."""
    dataset_name = os.path.basename(file_path).replace('.csv', '')
    print(f"\n{'='*80}")
    print(f"🔄 PROCESSING: {dataset_name.upper()}")
    print(f"{'='*80}")
    
    try:
        # Check for latest checkpoint first
        latest_checkpoint = get_latest_checkpoint(dataset_name)
        if latest_checkpoint:
            step = latest_checkpoint['step']
            print(f"📂 Found checkpoint at step {step}, resuming from there...")
            df = latest_checkpoint['dataframe']
            
            # Skip to the appropriate step
            if step >= 7:
                print(f"✅ {dataset_name.upper()} already processed completely!")
                return
        else:
            # Step 1: Load dataset
            print(f"📖 Step 1: Loading dataset...")
            df = pd.read_csv(file_path)
            print(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
            save_checkpoint(dataset_name, 1, df)
            
            # Step 2: Clean existing email columns
            print(f"🧹 Step 2: Cleaning existing email columns...")
            df = clean_existing_email_columns(df)
            save_checkpoint(dataset_name, 2, df)
            step = 2
        
        # Step 3: Find LinkedIn column and enrich
        if step < 3:
            linkedin_column = None
            possible_linkedin_cols = ['LinkedIn Profile', 'LinkedIn URL', 'linkedin_url', 'LinkedIn']
            for col in possible_linkedin_cols:
                if col in df.columns:
                    linkedin_column = col
                    break
            
            if not linkedin_column:
                print(f"  ❌ No LinkedIn column found. Skipping enrichment.")
                return
            
            print(f"  Using LinkedIn column: {linkedin_column}")
            
            # Step 3: Enrich with Clado API
            print(f"🔍 Step 3: Enriching with Clado API...")
            df = await enrich_with_clado(df, linkedin_column, dataset_name)
        
        # Step 4: Filter people with emails
        if step < 4:
            print(f"📧 Step 4: Filtering people with emails...")
            df = filter_people_with_emails(df)
            save_checkpoint(dataset_name, 4, df)
        
        # Step 5: Apply corporate email filtering
        if step < 5:
            print(f"🏢 Step 5: Applying corporate email filtering...")
            df = apply_corporate_email_filtering(df)
            save_checkpoint(dataset_name, 5, df)
        
        # Step 6: Consolidate to one email per person
        if step < 6:
            print(f"📝 Step 6: Consolidating to one email per person...")
            df = consolidate_to_one_email_per_person(df)
            save_checkpoint(dataset_name, 6, df)
        
        # Step 7: Save final result
        if step < 7:
            output_file = f"data/{dataset_name}_final_enriched.csv"
            df.to_csv(output_file, index=False)
            print(f"💾 Step 7: Saved final result to {output_file}")
            save_checkpoint(dataset_name, 7, df)
        
        # Step 8: Generate analysis
        print(f"📊 Step 8: Generating analysis...")
        analyze_final_results(df, dataset_name)
        
        print(f"✅ {dataset_name.upper()} processing complete!")
        
    except Exception as e:
        print(f"❌ Error processing {dataset_name}: {e}")
        import traceback
        traceback.print_exc()
        
        # Save error checkpoint
        try:
            save_checkpoint(dataset_name, f"error_{int(time.time())}", df if 'df' in locals() else None, {'error': str(e)})
        except:
            pass

def cleanup_old_checkpoints(dataset_name, keep_latest=3):
    """Clean up old checkpoint files, keeping only the latest few."""
    checkpoint_dir = "checkpoints"
    if not os.path.exists(checkpoint_dir):
        return
    
    checkpoint_files = glob.glob(f"{checkpoint_dir}/{dataset_name}_step_*.pkl")
    if len(checkpoint_files) <= keep_latest:
        return
    
    # Sort by modification time and remove older ones
    checkpoint_files.sort(key=os.path.getmtime, reverse=True)
    files_to_remove = checkpoint_files[keep_latest:]
    
    for file_path in files_to_remove:
        try:
            os.remove(file_path)
            print(f"  🗑️ Cleaned up old checkpoint: {os.path.basename(file_path)}")
        except:
            pass

async def main():
    """Main function to process all datasets with checkpointing."""
    print(f"🚀 COMPREHENSIVE EMAIL ENRICHMENT PIPELINE")
    print(f"{'='*80}")
    print(f"Priority: Personal > Work > Educational emails")
    print(f"✨ Features: Checkpointing, Error Recovery, Progress Tracking")
    print(f"{'='*80}")
    
    # Find all CSV files in data directory
    data_dir = "data"
    if not os.path.exists(data_dir):
        print(f"❌ Data directory '{data_dir}' not found!")
        return
    
    csv_files = [f for f in glob.glob(os.path.join(data_dir, "*.csv")) 
                 if not f.endswith('_enriched.csv') and not f.endswith('_final_enriched.csv')]
    
    if not csv_files:
        print(f"❌ No CSV files found in '{data_dir}' directory!")
        return
    
    print(f"📁 Found {len(csv_files)} datasets to process:")
    for file_path in csv_files:
        dataset_name = os.path.basename(file_path).replace('.csv', '')
        latest_checkpoint = get_latest_checkpoint(dataset_name)
        if latest_checkpoint:
            step = latest_checkpoint['step']
            timestamp = latest_checkpoint['timestamp']
            print(f"  • {os.path.basename(file_path)} (checkpoint: step {step}, {timestamp[:16]})")
        else:
            print(f"  • {os.path.basename(file_path)} (no checkpoint)")
    
    start_time = time.time()
    
    # Process each dataset
    for file_path in csv_files:
        dataset_name = os.path.basename(file_path).replace('.csv', '')
        
        try:
            await process_single_dataset(file_path)
            
            # Clean up old checkpoints for this dataset
            cleanup_old_checkpoints(dataset_name, keep_latest=2)
            
        except KeyboardInterrupt:
            print(f"\n⚠️ Process interrupted by user. Checkpoints saved for resuming later.")
            return
        except Exception as e:
            print(f"❌ Fatal error processing {dataset_name}: {e}")
            continue
    
    # Final summary
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\n{'='*80}")
    print(f"🎉 ALL DATASETS PROCESSED SUCCESSFULLY!")
    print(f"{'='*80}")
    print(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
    print(f"Datasets processed: {len(csv_files)}")
    print(f"\n📁 Final enriched files saved with '_final_enriched.csv' suffix")
    print(f"💾 Checkpoints saved in 'checkpoints/' directory")
    print(f"🔄 To resume interrupted processing, just run the script again")
    print(f"{'='*80}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n⚠️ Script interrupted. Checkpoints saved for resuming later.")