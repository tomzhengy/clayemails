import pandas as pd
import aiohttp
import asyncio
import time
import json
import sys
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Clado API configuration
API_BASE_URL = "https://search.clado.ai/api/enrich"
API_KEY = "lk_4b5dc217320e403b91ca1f28c4307921"

# Bulk API settings
MAX_URLS_PER_BATCH = 500  # Process 500 URLs at a time for better reliability
POLL_INTERVAL = 5  # Seconds between status checks
MAX_POLL_TIME = 3600  # Maximum time to poll for results (1 hour)

async def submit_bulk_job(session: aiohttp.ClientSession, 
                         linkedin_urls: List[str],
                         email_enrichment: bool = True,
                         phone_enrichment: bool = False) -> Optional[str]:
    """
    Submit a bulk enrichment job to Clado API.
    
    Args:
        session: aiohttp session for making requests
        linkedin_urls: List of LinkedIn profile URLs to enrich
        email_enrichment: Whether to only enrich emails
        phone_enrichment: Whether to only enrich phones
        
    Returns:
        Job ID if successful, None otherwise
    """
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
    """
    Poll the status of a bulk enrichment job.
    
    Args:
        session: aiohttp session for making requests
        job_id: The job ID to check
        
    Returns:
        Job status and results if successful, None otherwise
    """
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
    """
    Wait for a bulk enrichment job to complete, with progress updates.
    
    Args:
        session: aiohttp session for making requests
        job_id: The job ID to wait for
        total_urls: Total number of URLs being processed
        
    Returns:
        Final job results if successful, None otherwise
    """
    start_time = time.time()
    last_processed = 0
    milestone_count = 50
    next_milestone = milestone_count
    
    print(f"\n⏳ Job {job_id} submitted. Polling for results...")
    print(f"Will report progress every {milestone_count} profiles completed...\n")
    
    while True:
        # Check if we've exceeded maximum poll time
        if time.time() - start_time > MAX_POLL_TIME:
            print(f"\n❌ Job polling timeout after {MAX_POLL_TIME} seconds")
            return None
        
        # Poll job status
        status_data = await poll_job_status(session, job_id)
        
        if not status_data:
            return None
        
        status = status_data.get('status', 'unknown')
        processed = status_data.get('processed', 0)
        successful = status_data.get('successful', 0)
        failed = status_data.get('failed', 0)
        
        # Report progress at milestones
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
        
        # Check if job is complete
        if status in ['completed', 'error']:
            if status == 'completed':
                print(f"\n✅ Job {job_id} completed successfully!")
            else:
                print(f"\n❌ Job {job_id} failed with error")
            return status_data
        
        # Wait before next poll
        await asyncio.sleep(POLL_INTERVAL)

def parse_bulk_result(result: Dict) -> Dict[str, str]:
    """
    Parse a single result from bulk enrichment into flat columns.
    
    Args:
        result: Single result from bulk enrichment
        
    Returns:
        Dictionary of column_name: value pairs
    """
    parsed = {}
    
    # Check if enrichment was successful
    if not result.get('success'):
        parsed['Enrichment Status'] = f"Error: {result.get('error', 'Unknown error')}"
        return parsed
    
    # Extract data
    data = result.get('data', {})
    
    # Extract contacts
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
    
    # Add to result dictionary
    parsed['Work Email (Clado)'] = '; '.join(work_emails) if work_emails else ''
    parsed['Personal Email (Clado)'] = '; '.join(personal_emails) if personal_emails else ''
    parsed['Phone (Clado)'] = '; '.join(phones) if phones else ''
    
    # Extract social media profiles
    social_profiles = data.get('social', [])
    for profile in social_profiles:
        platform = profile.get('type')
        link = profile.get('link')
        rating = profile.get('rating')
        
        if platform == 'fb':
            parsed['Facebook Profile'] = link or ''
            parsed['Facebook Match Rating'] = rating or ''
        elif platform == 'tw':
            parsed['Twitter Profile'] = link or ''
            parsed['Twitter Match Rating'] = rating or ''
        elif platform == 'ig':
            parsed['Instagram Profile'] = link or ''
            parsed['Instagram Match Rating'] = rating or ''
    
    # Set enrichment status
    if contacts:
        parsed['Enrichment Status'] = 'Success'
    else:
        parsed['Enrichment Status'] = 'No contacts found'
    
    return parsed

async def enrich_csv_file_async(input_file: str, output_file: Optional[str] = None):
    """
    Asynchronously enrich all LinkedIn profiles in a CSV file using bulk API.
    Streams results to output CSV as each batch completes.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file (if None, adds '_enriched' to input filename)
    """
    try:
        # Read the CSV file
        print(f"Reading file: {input_file}")
        df = pd.read_csv(input_file)
        
        # Check if LinkedIn Profile column exists (handle both column names)
        linkedin_column = None
        if 'LinkedIn Profile' in df.columns:
            linkedin_column = 'LinkedIn Profile'
        elif 'LinkedIn URL' in df.columns:
            linkedin_column = 'LinkedIn URL'
        elif 'linkedin_url' in df.columns:
            linkedin_column = 'linkedin_url'
        elif 'LinkedIn' in df.columns:
            linkedin_column = 'LinkedIn'
        else:
            print("Error: No LinkedIn column found in CSV")
            print("Looking for: 'LinkedIn Profile', 'LinkedIn URL', 'linkedin_url', or 'LinkedIn'")
            print(f"Available columns: {list(df.columns)}")
            return None
        
        print(f"Using LinkedIn column: {linkedin_column}")
        
        # Initialize new columns
        new_columns = [
            'Enrichment Status',
            'Work Email (Clado)',
            'Personal Email (Clado)',
            'Phone (Clado)',
            'Facebook Profile',
            'Facebook Match Rating',
            'Twitter Profile',
            'Twitter Match Rating',
            'Instagram Profile',
            'Instagram Match Rating'
        ]
        
        for col in new_columns:
            df[col] = ''
        
        # Determine output filename
        if output_file is None:
            base_name = input_file.rsplit('.', 1)[0]
            output_file = f"{base_name}_enriched.csv"
        
        print(f"📝 Streaming results to: {output_file}")
        
        # Track start time
        start_time = time.time()
        
        # Process rows
        total_rows = len(df)
        print(f"Found {total_rows} rows to enrich")
        
        # Open output CSV file for streaming
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            # Write header
            writer = csv.DictWriter(csvfile, fieldnames=df.columns.tolist())
            writer.writeheader()
            csvfile.flush()
            
            # First, write all rows without LinkedIn URLs
            no_url_count = 0
            for idx in range(total_rows):
                linkedin_url = df.loc[idx, linkedin_column]
                if pd.isna(linkedin_url) or not linkedin_url:
                    df.loc[idx, 'Enrichment Status'] = 'No LinkedIn URL'
                    writer.writerow(df.loc[idx].to_dict())
                    no_url_count += 1
            
            if no_url_count > 0:
                csvfile.flush()
                print(f"💾 Written {no_url_count} rows without LinkedIn URLs to output file")
            
            # Create aiohttp session
            async with aiohttp.ClientSession() as session:
                # Collect all valid LinkedIn URLs with their indices
                url_to_index = {}
                valid_urls = []
                
                for idx in range(total_rows):
                    linkedin_url = df.loc[idx, linkedin_column]
                    if not (pd.isna(linkedin_url) or not linkedin_url):
                        url_to_index[linkedin_url] = idx
                        valid_urls.append(linkedin_url)
                
                if not valid_urls:
                    print("No valid LinkedIn URLs found to process")
                    return df
                
                print(f"Found {len(valid_urls)} profiles with LinkedIn URLs")
                
                # Calculate total batches needed
                total_batches = (len(valid_urls) + MAX_URLS_PER_BATCH - 1) // MAX_URLS_PER_BATCH
                if total_batches > 1:
                    print(f"Will process in {total_batches} batches of up to {MAX_URLS_PER_BATCH} URLs each")
                
                # Process in batches
                total_written = no_url_count
                
                for batch_start in range(0, len(valid_urls), MAX_URLS_PER_BATCH):
                    batch_end = min(batch_start + MAX_URLS_PER_BATCH, len(valid_urls))
                    batch_urls = valid_urls[batch_start:batch_end]
                    
                    batch_num = (batch_start // MAX_URLS_PER_BATCH) + 1
                    
                    if total_batches > 1:
                        print(f"\n{'='*60}")
                        print(f"📦 BATCH {batch_num}/{total_batches}")
                        print(f"Processing URLs {batch_start + 1} to {batch_end} ({len(batch_urls)} URLs)")
                        print(f"{'='*60}")
                    
                    # Submit bulk job
                    job_id = await submit_bulk_job(session, batch_urls)
                    
                    if not job_id:
                        print(f"❌ Failed to submit batch {batch_num}")
                        # Write failed batch with error status
                        for url in batch_urls:
                            if url in url_to_index:
                                idx = url_to_index[url]
                                df.loc[idx, 'Enrichment Status'] = 'Error: Batch submission failed'
                                writer.writerow(df.loc[idx].to_dict())
                                total_written += 1
                        csvfile.flush()
                        continue
                    
                    print(f"✅ Bulk job submitted with ID: {job_id}")
                    
                    # Wait for job completion
                    job_results = await wait_for_job_completion(session, job_id, len(batch_urls))
                    
                    if job_results:
                        # Process batch results and write to CSV immediately
                        batch_results = job_results.get('results', [])
                        batch_written = 0
                        
                        for result in batch_results:
                            linkedin_url = result.get('linkedin_url')
                            if linkedin_url and linkedin_url in url_to_index:
                                idx = url_to_index[linkedin_url]
                                parsed_data = parse_bulk_result(result)
                                for col, value in parsed_data.items():
                                    if col in df.columns:
                                        df.loc[idx, col] = value
                                
                                # Write enriched row to CSV
                                writer.writerow(df.loc[idx].to_dict())
                                batch_written += 1
                        
                        csvfile.flush()
                        total_written += batch_written
                        
                        if total_batches > 1:
                            print(f"✅ Batch {batch_num}/{total_batches} complete")
                            print(f"💾 Written {batch_written} rows from this batch")
                            print(f"📊 Total rows written so far: {total_written}/{total_rows}")
                    else:
                        # Write failed batch with error status
                        for url in batch_urls:
                            if url in url_to_index:
                                idx = url_to_index[url]
                                df.loc[idx, 'Enrichment Status'] = 'Error: Job failed'
                                writer.writerow(df.loc[idx].to_dict())
                                total_written += 1
                        csvfile.flush()
        
        # Calculate processing time
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"\n{'='*60}")
        print(f"ENRICHMENT COMPLETE - ALL RESULTS SAVED")
        print(f"{'='*60}")
        print(f"✅ Output file: {output_file}")
        print(f"📊 Total rows written: {total_written}/{total_rows}")
        print(f"Total processing time: {processing_time:.2f} seconds")
        if len(valid_urls) > 0:
            print(f"Average time per profile: {processing_time/len(valid_urls):.2f} seconds")
            print(f"Processing rate: {len(valid_urls)/processing_time:.1f} profiles/sec")
        
        # Print summary
        success_count = len(df[df['Enrichment Status'] == 'Success'])
        no_contacts_count = len(df[df['Enrichment Status'] == 'No contacts found'])
        error_count = len(df[df['Enrichment Status'].str.contains('Error', na=False)])
        no_url_count = len(df[df['Enrichment Status'] == 'No LinkedIn URL'])
        
        print(f"\n📊 Enrichment Summary:")
        print(f"  ✅ Successfully enriched: {success_count}")
        print(f"  ⚠️  No contacts found: {no_contacts_count}")
        print(f"  ❌ Errors: {error_count}")
        print(f"  🔗 No LinkedIn URL: {no_url_count}")
        print(f"  📝 Total rows: {total_rows}")
        print(f"\n💡 Results were streamed to CSV in real-time")
        print(f"   If the script was interrupted, partial results are already saved!")
        print(f"{'='*60}")
        
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function to run the async enrichment."""
    # Default input file
    input_file = "published_authors_only_20250819_195433.csv"
    
    # Check if a filename was provided as command line argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # Optional: specify output file as second argument
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Run the async enrichment
    asyncio.run(enrich_csv_file_async(input_file, output_file))

if __name__ == "__main__":
    main()