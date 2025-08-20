#!/usr/bin/env python3
"""
Filter corporate email domains from output_with_emails CSV.
Keeps corporate emails only if they're the person's only email.
Gmail, Yahoo, Hotmail and similar are treated as personal emails.
"""

import pandas as pd
import sys
from datetime import datetime

# Define personal email domains (these are NOT corporate)
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

# Academic domains (also not corporate in traditional sense)
ACADEMIC_EXTENSIONS = {'.edu', '.ac.uk', '.ac.jp', '.ac.cn', '.ac.in', '.edu.au', '.edu.cn'}

def is_personal_email(email):
    """Check if an email is from a personal domain."""
    if not email or '@' not in email:
        return False
    
    email = email.strip().lower()
    domain = email.split('@')[1] if '@' in email else ''
    
    # Check if it's a known personal domain
    if domain in PERSONAL_DOMAINS:
        return True
    
    # Check if it's an academic domain
    if any(domain.endswith(ext) for ext in ACADEMIC_EXTENSIONS):
        return True
    
    # Check for university patterns
    if any(term in domain for term in ['university', 'college', 'institute', 'academia']):
        return True
    
    return False

def is_corporate_email(email):
    """Check if an email is from a corporate domain."""
    return not is_personal_email(email)

def process_email_field(email_field):
    """Process an email field that may contain multiple emails separated by semicolons."""
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

def filter_corporate_emails(input_file='another.csv', output_file=None):
    """
    Filter corporate emails from CSV, keeping them only if they're the person's only email.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file (if None, creates filtered version)
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
        
        # Initialize counters
        people_with_only_corporate = 0
        people_with_personal = 0
        people_with_both = 0
        people_with_no_email_after_filter = 0
        
        corporate_domains_seen = set()
        personal_domains_seen = set()
        
        # Process each row
        print(f"\n🔍 Processing emails...")
        
        for idx in range(total_rows):
            # Get all emails for this person
            work_emails_str = df.loc[idx, work_email_col] if work_email_col in df.columns else ''
            personal_emails_str = df.loc[idx, personal_email_col] if personal_email_col in df.columns else ''
            
            # Process work emails
            work_personal, work_corporate = process_email_field(work_emails_str)
            
            # Process personal emails column
            personal_personal, personal_corporate = process_email_field(personal_emails_str)
            
            # Combine all emails
            all_personal = work_personal + personal_personal
            all_corporate = work_corporate + personal_corporate
            
            # Track domains
            for email in all_personal:
                if '@' in email:
                    personal_domains_seen.add(email.split('@')[1].lower())
            for email in all_corporate:
                if '@' in email:
                    corporate_domains_seen.add(email.split('@')[1].lower())
            
            # Determine what to keep
            if all_personal:
                # Person has personal emails, so we'll use only those
                people_with_personal += 1
                if all_corporate:
                    people_with_both += 1
                
                # Update the dataframe to keep only personal emails
                # Redistribute personal emails back to the columns
                if work_email_col in df.columns:
                    df.loc[idx, work_email_col] = '; '.join(work_personal) if work_personal else ''
                if personal_email_col in df.columns:
                    df.loc[idx, personal_email_col] = '; '.join(personal_personal) if personal_personal else ''
                    
            elif all_corporate:
                # Person only has corporate emails, keep them
                people_with_only_corporate += 1
                # Keep the corporate emails as they are
                
            else:
                # No emails at all (shouldn't happen in filtered file)
                people_with_no_email_after_filter += 1
        
        # Remove rows that now have no emails
        if work_email_col in df.columns and personal_email_col in df.columns:
            has_any_email = (df[work_email_col].notna() & (df[work_email_col] != '')) | \
                           (df[personal_email_col].notna() & (df[personal_email_col] != ''))
            df_filtered = df[has_any_email].copy()
        else:
            df_filtered = df.copy()
        
        # Determine output filename
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"another{timestamp}.csv"
        
        # Save filtered dataframe
        df_filtered.to_csv(output_file, index=False)
        
        # Print results
        print(f"\n{'='*60}")
        print(f"📊 CORPORATE EMAIL FILTERING COMPLETE")
        print(f"{'='*60}")
        
        print(f"\n📈 Statistics:")
        print(f"   Total people processed: {total_rows}")
        print(f"   People with personal emails: {people_with_personal} ({people_with_personal/total_rows*100:.1f}%)")
        print(f"   People with ONLY corporate emails: {people_with_only_corporate} ({people_with_only_corporate/total_rows*100:.1f}%)")
        print(f"   People with both (kept personal only): {people_with_both} ({people_with_both/total_rows*100:.1f}%)")
        
        print(f"\n🏢 Corporate Email Analysis:")
        print(f"   People who ONLY have corporate emails: {people_with_only_corporate}")
        print(f"   These people's corporate emails were KEPT")
        
        if corporate_domains_seen:
            print(f"\n   Top 20 corporate domains encountered:")
            domain_counts = {}
            # Recount corporate domains from original data
            for idx in range(total_rows):
                work_emails_str = df.loc[idx, work_email_col] if work_email_col in df.columns else ''
                personal_emails_str = df.loc[idx, personal_email_col] if personal_email_col in df.columns else ''
                _, work_corporate = process_email_field(work_emails_str)
                _, personal_corporate = process_email_field(personal_emails_str)
                for email in work_corporate + personal_corporate:
                    if '@' in email:
                        domain = email.split('@')[1].lower()
                        domain_counts[domain] = domain_counts.get(domain, 0) + 1
            
            sorted_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:20]
            for domain, count in sorted_domains:
                print(f"      {domain:30} ({count} emails)")
        
        print(f"\n📧 Personal/Academic domains treated as non-corporate:")
        common_personal = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', '*.edu']
        print(f"   Including: {', '.join(common_personal)}, and academic domains")
        
        print(f"\n✅ Output saved to: {output_file}")
        print(f"   Total rows in output: {len(df_filtered)}")
        print(f"   Rows removed: {total_rows - len(df_filtered)}")
        
        print(f"{'='*60}")
        
        return df_filtered, people_with_only_corporate
        
    except FileNotFoundError:
        print(f"❌ Error: File '{input_file}' not found")
        return None, 0
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None, 0

def main():
    """Main function to run the corporate email filter."""
    # Default input file (the filtered file with emails)
    input_file = 'another.csv'
    
    # Check if a filename was provided as command line argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # Optional: specify output file as second argument
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Run the filter
    df_filtered, corporate_only_count = filter_corporate_emails(input_file, output_file)
    
    if df_filtered is not None:
        print(f"\n💡 Summary: {corporate_only_count} people only have corporate emails (these were kept)")

if __name__ == "__main__":
    main()
