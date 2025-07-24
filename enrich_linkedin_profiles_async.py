import pandas as pd
import aiohttp
import asyncio
import time
import json
import sys
from typing import Dict, List, Optional, Tuple

# Clado API configuration
API_BASE_URL = "https://search.clado.ai/api/enrich/contacts"
API_KEY = "lk_9c1512d5d7214191aab1ad1e5d7d96e9"

# Concurrency settings
MAX_CONCURRENT_REQUESTS = 10  # Reduced from 10 to avoid rate limits
RATE_LIMIT_DELAY = 1.0  # Increased from 0.1 to 1 second between requests
MAX_RETRIES = 3  # Maximum number of retries for rate-limited requests
INITIAL_BACKOFF = 5  # Initial backoff time in seconds

async def enrich_linkedin_profile(session: aiohttp.ClientSession, 
                                 linkedin_url: str, 
                                 semaphore: asyncio.Semaphore,
                                 row_idx: int) -> Tuple[int, Dict]:
    """
    Asynchronously call Clado API to enrich a LinkedIn profile URL.
    
    Args:
        session: aiohttp session for making requests
        linkedin_url: LinkedIn profile URL to enrich
        semaphore: Semaphore to limit concurrent requests
        row_idx: Row index in the dataframe
        
    Returns:
        Tuple of (row_index, enrichment_data)
    """
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    
    params = {
        "linkedin_url": linkedin_url
    }
    
    async with semaphore:  # Limit concurrent requests
        retry_count = 0
        backoff_time = INITIAL_BACKOFF
        
        while retry_count <= MAX_RETRIES:
            try:
                async with session.get(API_BASE_URL, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        result = data.get('data', [{}])[0] if data.get('data') else {}
                        # Small delay to prevent overwhelming the API
                        await asyncio.sleep(RATE_LIMIT_DELAY)
                        return (row_idx, result)
                    elif response.status == 401:
                        text = await response.text()
                        print(f"Authentication error for row {row_idx + 1}: {text}")
                        result = {"error": "Authentication failed"}
                        return (row_idx, result)
                    elif response.status == 402:
                        text = await response.text()
                        print(f"Insufficient credits for row {row_idx + 1}: {text}")
                        result = {"error": "Insufficient credits"}
                        return (row_idx, result)
                    elif response.status == 429:
                        # Rate limited - parse retry time from response
                        text = await response.text()
                        print(f"Rate limited for row {row_idx + 1}. Retry {retry_count + 1}/{MAX_RETRIES}")
                        
                        # Try to parse the retry time from the error message
                        import re
                        match = re.search(r'Try again in (\d+) seconds', text)
                        if match:
                            wait_time = int(match.group(1)) + 2  # Add 2 seconds buffer
                            print(f"  Waiting {wait_time} seconds as requested by API...")
                            await asyncio.sleep(wait_time)
                        else:
                            # Use exponential backoff if can't parse retry time
                            print(f"  Waiting {backoff_time} seconds...")
                            await asyncio.sleep(backoff_time)
                            backoff_time *= 2  # Double the backoff time
                        
                        retry_count += 1
                        continue
                    else:
                        text = await response.text()
                        print(f"Error for row {row_idx + 1} ({linkedin_url}): {response.status} - {text}")
                        result = {"error": f"HTTP {response.status}"}
                        return (row_idx, result)
                        
            except Exception as e:
                print(f"Exception for row {row_idx + 1} ({linkedin_url}): {str(e)}")
                return (row_idx, {"error": str(e)})
        
        # If we've exhausted all retries
        print(f"Failed after {MAX_RETRIES} retries for row {row_idx + 1}")
        return (row_idx, {"error": "Max retries exceeded"})

def parse_enrichment_data(enrichment_data: Dict) -> Dict[str, str]:
    """
    Parse the enrichment data into flat columns for CSV.
    
    Args:
        enrichment_data: Raw enrichment data from API
        
    Returns:
        Dictionary of column_name: value pairs
    """
    result = {}
    
    # Handle error cases
    if enrichment_data.get('error'):
        result['Enrichment Status'] = f"Error: {enrichment_data.get('message', enrichment_data.get('error'))}"
        return result
    
    # Extract contacts
    contacts = enrichment_data.get('contacts', [])
    
    # Extract emails
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
    result['Work Email (Clado)'] = '; '.join(work_emails) if work_emails else ''
    result['Personal Email (Clado)'] = '; '.join(personal_emails) if personal_emails else ''
    result['Phone (Clado)'] = '; '.join(phones) if phones else ''
    
    # Extract social media profiles
    social_profiles = enrichment_data.get('social', [])
    for profile in social_profiles:
        platform = profile.get('type')
        link = profile.get('link')
        rating = profile.get('rating')
        
        if platform == 'fb':
            result['Facebook Profile'] = link or ''
            result['Facebook Match Rating'] = rating or ''
        elif platform == 'tw':
            result['Twitter Profile'] = link or ''
            result['Twitter Match Rating'] = rating or ''
        elif platform == 'ig':
            result['Instagram Profile'] = link or ''
            result['Instagram Match Rating'] = rating or ''
    
    # Add enrichment status
    if any(contacts):
        result['Enrichment Status'] = 'Success'
    else:
        result['Enrichment Status'] = 'No contacts found'
    
    return result

async def process_batch(session: aiohttp.ClientSession,
                       df: pd.DataFrame,
                       batch_indices: List[int],
                       semaphore: asyncio.Semaphore,
                       linkedin_column: str) -> List[Tuple[int, Dict]]:
    """
    Process a batch of profiles asynchronously.
    
    Args:
        session: aiohttp session
        df: DataFrame with profile data
        batch_indices: List of row indices to process
        semaphore: Semaphore to limit concurrent requests
        linkedin_column: Name of the LinkedIn URL column
        
    Returns:
        List of (row_index, enrichment_data) tuples
    """
    tasks = []
    
    for idx in batch_indices:
        linkedin_url = df.loc[idx, linkedin_column]
        
        # Skip if no LinkedIn URL
        if pd.isna(linkedin_url) or not linkedin_url:
            continue
            
        task = enrich_linkedin_profile(session, linkedin_url, semaphore, idx)
        tasks.append(task)
    
    # Wait for all tasks in batch to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out exceptions and return valid results
    valid_results = []
    for result in results:
        if isinstance(result, Exception):
            print(f"Task exception: {result}")
        else:
            valid_results.append(result)
    
    return valid_results

async def enrich_csv_file_async(input_file: str, output_file: Optional[str] = None):
    """
    Asynchronously enrich all LinkedIn profiles in a CSV file.
    
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
        elif 'linkedin_url' in df.columns:
            linkedin_column = 'linkedin_url'
        elif 'LinkedIn' in df.columns:
            linkedin_column = 'LinkedIn'
        else:
            print("Error: Neither 'LinkedIn Profile' nor 'linkedin_url' nor 'LinkedIn' column found in CSV")
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
        
        # Track start time
        start_time = time.time()
        
        # Process rows
        total_rows = len(df)
        print(f"Found {total_rows} rows to enrich")
        print(f"Processing with {MAX_CONCURRENT_REQUESTS} concurrent requests...")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        # Create aiohttp session
        async with aiohttp.ClientSession() as session:
            # Process all rows with valid LinkedIn URLs
            valid_indices = []
            for idx in range(total_rows):
                linkedin_url = df.loc[idx, linkedin_column]
                if pd.isna(linkedin_url) or not linkedin_url:
                    df.loc[idx, 'Enrichment Status'] = 'No LinkedIn URL'
                else:
                    valid_indices.append(idx)
            
            print(f"Processing {len(valid_indices)} profiles with LinkedIn URLs...")
            
            # Process in batches for progress reporting
            batch_size = 50
            processed_count = 0
            
            for i in range(0, len(valid_indices), batch_size):
                batch_indices = valid_indices[i:i + batch_size]
                batch_results = await process_batch(session, df, batch_indices, semaphore, linkedin_column)
                
                # Update dataframe with results
                for row_idx, enrichment_data in batch_results:
                    parsed_data = parse_enrichment_data(enrichment_data)
                    for col, value in parsed_data.items():
                        df.loc[row_idx, col] = value
                
                processed_count += len(batch_indices)
                print(f"Progress: {processed_count}/{len(valid_indices)} profiles processed...")
        
        # Determine output filename
        if output_file is None:
            base_name = input_file.rsplit('.', 1)[0]
            output_file = f"{base_name}_enriched.csv"
        
        # Save the enriched dataframe
        df.to_csv(output_file, index=False)
        
        # Calculate processing time
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"\nEnriched file saved to: {output_file}")
        print(f"Total processing time: {processing_time:.2f} seconds")
        print(f"Average time per profile: {processing_time/len(valid_indices):.2f} seconds")
        
        # Print summary
        success_count = len(df[df['Enrichment Status'] == 'Success'])
        no_contacts_count = len(df[df['Enrichment Status'] == 'No contacts found'])
        error_count = len(df[df['Enrichment Status'].str.contains('Error', na=False)])
        no_url_count = len(df[df['Enrichment Status'] == 'No LinkedIn URL'])
        
        print(f"\nEnrichment Summary:")
        print(f"  Successfully enriched: {success_count}")
        print(f"  No contacts found: {no_contacts_count}")
        print(f"  Errors: {error_count}")
        print(f"  No LinkedIn URL: {no_url_count}")
        print(f"  Total processed: {total_rows}")
        
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def main():
    """Main function to run the async enrichment."""
    # Default input file (the cleaned file from previous script)
    input_file = "Targeted-Tech-and-Biotech-Executives-Default-view-export-1751422669424_no_emails.csv"
    
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