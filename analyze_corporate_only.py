#!/usr/bin/env python3
"""
Analyze people who only have corporate emails in the dataset.
Shows which companies these people work at based on their email domains.
"""

import pandas as pd
import sys
from collections import Counter

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

def analyze_corporate_only(input_file='output_with_emails_20250820_014239.csv'):
    """
    Analyze people who only have corporate emails.
    
    Args:
        input_file: Path to input CSV file
    """
    try:
        # Read the CSV file
        print(f"📖 Reading file: {input_file}")
        df = pd.read_csv(input_file)
        total_rows = len(df)
        print(f"   Total rows: {total_rows}")
        
        # Email columns
        work_email_col = 'Work Email (Clado)'
        personal_email_col = 'Personal Email (Clado)'
        
        # Track people with only corporate emails
        corporate_only_people = []
        corporate_domains_counter = Counter()
        company_counter = Counter()
        
        print(f"\n🔍 Analyzing corporate-only emails...")
        
        for idx, row in df.iterrows():
            # Get all emails for this person
            all_emails = []
            
            # Collect work emails
            if pd.notna(row.get(work_email_col)) and row.get(work_email_col):
                emails = str(row[work_email_col]).split(';')
                all_emails.extend([e.strip() for e in emails if e.strip() and '@' in e])
            
            # Collect personal emails column
            if pd.notna(row.get(personal_email_col)) and row.get(personal_email_col):
                emails = str(row[personal_email_col]).split(';')
                all_emails.extend([e.strip() for e in emails if e.strip() and '@' in e])
            
            # Check if all emails are corporate
            if all_emails:
                personal_count = sum(1 for email in all_emails if is_personal_email(email))
                corporate_emails = [email for email in all_emails if not is_personal_email(email)]
                
                if personal_count == 0 and corporate_emails:
                    # This person only has corporate emails
                    person_info = {
                        'name': row.get('Full Name', 'Unknown'),
                        'company': row.get('Company', 'Unknown'),
                        'emails': corporate_emails,
                        'domains': list(set(email.split('@')[1].lower() for email in corporate_emails if '@' in email))
                    }
                    corporate_only_people.append(person_info)
                    
                    # Count domains
                    for domain in person_info['domains']:
                        corporate_domains_counter[domain] += 1
                    
                    # Count companies
                    if pd.notna(row.get('Company')):
                        company_counter[row['Company']] += 1
        
        # Print results
        print(f"\n{'='*60}")
        print(f"🏢 CORPORATE-ONLY EMAIL ANALYSIS")
        print(f"{'='*60}")
        
        print(f"\n📊 Summary:")
        print(f"   Total people analyzed: {total_rows}")
        print(f"   People with ONLY corporate emails: {len(corporate_only_people)}")
        print(f"   Percentage: {len(corporate_only_people)/total_rows*100:.1f}%")
        
        if corporate_domains_counter:
            print(f"\n🏢 Top 30 Corporate Domains (from people with ONLY corporate emails):")
            for domain, count in corporate_domains_counter.most_common(30):
                print(f"   {domain:35} {count:4} people")
        
        if company_counter:
            print(f"\n🏭 Top 30 Companies (based on Company field for corporate-only people):")
            for company, count in company_counter.most_common(30):
                if pd.notna(company) and company != 'nan':
                    print(f"   {company[:40]:40} {count:4} people")
        
        # Show some examples
        print(f"\n📋 Sample of people with ONLY corporate emails (first 10):")
        for i, person in enumerate(corporate_only_people[:10], 1):
            print(f"\n   {i}. {person['name']}")
            if person['company'] != 'Unknown' and pd.notna(person['company']):
                print(f"      Company: {person['company']}")
            print(f"      Email(s): {', '.join(person['emails'][:2])}")  # Show max 2 emails
            if len(person['emails']) > 2:
                print(f"                (and {len(person['emails'])-2} more)")
        
        # Export list of corporate-only people
        if corporate_only_people:
            output_file = 'corporate_only_analysis.csv'
            corporate_df = pd.DataFrame([
                {
                    'Name': p['name'],
                    'Company': p['company'],
                    'Corporate Emails': '; '.join(p['emails']),
                    'Domains': '; '.join(p['domains'])
                }
                for p in corporate_only_people
            ])
            corporate_df.to_csv(output_file, index=False)
            print(f"\n💾 Detailed list exported to: {output_file}")
        
        print(f"\n{'='*60}")
        
        return corporate_only_people
        
    except FileNotFoundError:
        print(f"❌ Error: File '{input_file}' not found")
        return []
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return []

def main():
    """Main function."""
    # Default input file
    input_file = 'output_with_emails_20250820_014239.csv'
    
    # Check if a filename was provided as command line argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # Run the analysis
    corporate_only = analyze_corporate_only(input_file)
    
    print(f"\n✨ Analysis complete: {len(corporate_only)} people have ONLY corporate emails")

if __name__ == "__main__":
    main()

