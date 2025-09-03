#!/usr/bin/env python3
"""
Reorder CSV file to prioritize by email type:
1. Personal emails (Gmail, Yahoo, Hotmail, etc.) - at the top
2. Work/Corporate emails - in the middle  
3. Educational emails (.edu domains) - at the bottom
"""

import pandas as pd
import sys
from datetime import datetime

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

def get_email_priority(email):
    """
    Determine priority of an email:
    1 = Personal (highest priority)
    2 = Work/Corporate (medium priority)
    3 = Educational (lowest priority)
    999 = No email
    """
    if pd.isna(email) or not email or email == '' or '@' not in str(email):
        return 999
    
    email = str(email).strip().lower()
    domain = email.split('@')[1] if '@' in email else ''
    
    # Check if it's a personal domain
    if domain in PERSONAL_DOMAINS:
        return 1
    
    # Check if it's an academic domain
    if any(domain.endswith(ext) for ext in ACADEMIC_EXTENSIONS):
        return 3
    
    # Check for university patterns
    if any(term in domain for term in ['university', 'college', 'institute', 'academia']):
        return 3
    
    # Otherwise it's a work/corporate email
    return 2

def reorder_csv_by_email_priority(input_file, output_file=None):
    """
    Reorder CSV file based on email priority.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file (if None, creates reordered version)
    """
    try:
        # Read the CSV file
        print(f"📖 Reading file: {input_file}")
        df = pd.read_csv(input_file)
        total_rows = len(df)
        print(f"   Total rows: {total_rows}")
        
        # Find the email column (could be 'Consolidated_Email' from pipeline or other columns)
        email_column = None
        possible_email_cols = ['Consolidated_Email', 'Email', 'Work Email (Clado)', 'Personal Email (Clado)']
        
        for col in possible_email_cols:
            if col in df.columns:
                email_column = col
                break
        
        if not email_column:
            print("❌ Error: No email column found")
            print(f"   Available columns: {list(df.columns)[:10]}...")
            return None
        
        print(f"   Using email column: {email_column}")
        
        # Add priority column
        print(f"\n🔢 Calculating email priorities...")
        df['_email_priority'] = df[email_column].apply(get_email_priority)
        
        # Count each type
        personal_count = len(df[df['_email_priority'] == 1])
        work_count = len(df[df['_email_priority'] == 2])
        edu_count = len(df[df['_email_priority'] == 3])
        no_email_count = len(df[df['_email_priority'] == 999])
        
        print(f"\n📊 Email Distribution:")
        print(f"   Personal emails: {personal_count} ({personal_count/total_rows*100:.1f}%)")
        print(f"   Work emails: {work_count} ({work_count/total_rows*100:.1f}%)")
        print(f"   Educational emails: {edu_count} ({edu_count/total_rows*100:.1f}%)")
        if no_email_count > 0:
            print(f"   No email: {no_email_count} ({no_email_count/total_rows*100:.1f}%)")
        
        # Sort by priority (1=personal first, 2=work, 3=edu, 999=no email last)
        print(f"\n🔄 Reordering rows by email priority...")
        df_sorted = df.sort_values('_email_priority')
        
        # Remove the temporary priority column
        df_sorted = df_sorted.drop('_email_priority', axis=1)
        
        # Determine output filename
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = input_file.rsplit('.', 1)[0]
            output_file = f"{base_name}_prioritized_{timestamp}.csv"
        
        # Save the reordered dataframe
        df_sorted.to_csv(output_file, index=False)
        
        # Print results
        print(f"\n{'='*60}")
        print(f"✅ REORDERING COMPLETE")
        print(f"{'='*60}")
        print(f"📁 Output file: {output_file}")
        print(f"📊 Order of rows:")
        print(f"   1. Personal emails: rows 1-{personal_count}")
        if work_count > 0:
            print(f"   2. Work emails: rows {personal_count+1}-{personal_count+work_count}")
        if edu_count > 0:
            print(f"   3. Educational emails: rows {personal_count+work_count+1}-{personal_count+work_count+edu_count}")
        if no_email_count > 0:
            print(f"   4. No email: rows {personal_count+work_count+edu_count+1}-{total_rows}")
        print(f"{'='*60}")
        
        # Show sample of reordered data
        print(f"\n📋 Sample of reordered data (first 5 rows):")
        for idx, row in df_sorted.head(5).iterrows():
            name = row.get('Full Name', row.get('Name', f'Row {idx}'))
            email = row.get(email_column, '')
            print(f"   • {name}: {email}")
        
        return df_sorted
        
    except FileNotFoundError:
        print(f"❌ Error: File '{input_file}' not found")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function to run the reordering."""
    # Default input file
    input_file = 'data/output_final_enriched.csv'
    
    # Check if a filename was provided as command line argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # Optional: specify output file as second argument
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Run the reordering
    reorder_csv_by_email_priority(input_file, output_file)

if __name__ == "__main__":
    main()