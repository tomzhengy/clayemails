#!/usr/bin/env python3
"""
Filter output.csv to keep only people with emails.
This script reads output.csv and creates a new file with only the rows
where either Work Email or Personal Email is present.
"""

import pandas as pd
import sys
from datetime import datetime

def filter_people_with_emails(input_file='output.csv', output_file=None):
    """
    Filter CSV to keep only rows with email addresses.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file (if None, creates output_with_emails.csv)
    """
    try:
        # Read the CSV file
        print(f"📖 Reading file: {input_file}")
        df = pd.read_csv(input_file)
        total_rows = len(df)
        print(f"   Total rows: {total_rows}")
        
        # Check if email columns exist
        work_email_col = 'Work Email (Clado)'
        personal_email_col = 'Personal Email (Clado)'
        
        if work_email_col not in df.columns or personal_email_col not in df.columns:
            print(f"❌ Error: Email columns not found")
            print(f"   Looking for: '{work_email_col}' and '{personal_email_col}'")
            print(f"   Available columns: {list(df.columns)[:10]}...")
            return None
        
        # Filter for rows with emails
        # Keep rows where either work email or personal email is not empty/NaN
        print(f"\n🔍 Filtering for people with emails...")
        
        # Count before filtering
        has_work_email = df[work_email_col].notna() & (df[work_email_col] != '')
        has_personal_email = df[personal_email_col].notna() & (df[personal_email_col] != '')
        has_any_email = has_work_email | has_personal_email
        
        work_email_count = has_work_email.sum()
        personal_email_count = has_personal_email.sum()
        both_emails_count = (has_work_email & has_personal_email).sum()
        any_email_count = has_any_email.sum()
        
        print(f"\n📊 Email Statistics:")
        print(f"   People with work email: {work_email_count}")
        print(f"   People with personal email: {personal_email_count}")
        print(f"   People with both types: {both_emails_count}")
        print(f"   People with any email: {any_email_count}")
        print(f"   People without any email: {total_rows - any_email_count}")
        
        # Filter dataframe
        df_with_emails = df[has_any_email].copy()
        
        # Determine output filename
        if output_file is None:
            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"output_with_emails_{timestamp}.csv"
        
        # Save filtered dataframe
        df_with_emails.to_csv(output_file, index=False)
        
        # Print results
        print(f"\n✅ Filtering Complete!")
        print(f"{'='*60}")
        print(f"📁 Output file: {output_file}")
        print(f"📊 Results:")
        print(f"   Original rows: {total_rows}")
        print(f"   Rows with emails: {len(df_with_emails)}")
        print(f"   Rows removed: {total_rows - len(df_with_emails)}")
        print(f"   Retention rate: {len(df_with_emails)/total_rows*100:.1f}%")
        print(f"{'='*60}")
        
        # Show sample of filtered data
        if len(df_with_emails) > 0:
            print(f"\n📋 Sample of people with emails (first 5):")
            sample_cols = ['Full Name', 'Company', work_email_col, personal_email_col]
            sample_cols = [col for col in sample_cols if col in df_with_emails.columns]
            
            for idx, row in df_with_emails.head(5).iterrows():
                print(f"\n   {idx + 1}. {row.get('Full Name', 'N/A')}")
                if 'Company' in row:
                    print(f"      Company: {row['Company']}")
                if row.get(work_email_col):
                    print(f"      Work: {row[work_email_col]}")
                if row.get(personal_email_col):
                    print(f"      Personal: {row[personal_email_col]}")
        
        return df_with_emails
        
    except FileNotFoundError:
        print(f"❌ Error: File '{input_file}' not found")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function to run the email filter."""
    # Default input file
    input_file = 'output.csv'
    
    # Check if a filename was provided as command line argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # Optional: specify output file as second argument
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Run the filter
    filter_people_with_emails(input_file, output_file)

if __name__ == "__main__":
    main()
