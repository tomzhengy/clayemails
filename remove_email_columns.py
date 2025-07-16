import pandas as pd
import sys

def remove_email_columns(input_file, output_file=None):
    """
    Remove all email-related columns from a CSV file.
    
    Args:
        input_file: Path to the input CSV file
        output_file: Path to the output CSV file (if None, adds '_no_emails' to input filename)
    """
    try:
        # Read the CSV file
        print(f"Reading file: {input_file}")
        df = pd.read_csv(input_file)
        
        # List of email-related columns to remove
        email_columns = [
            "Find work email",
            "Find Work Email",
            "Find Work Email (2)",
            "Find work email (2)",
            "Find work email (3)",
            "Find Work Email (3)",
            "Find email",
            "Work Email"
        ]
        
        # Get the columns that exist in the dataframe
        columns_to_drop = [col for col in email_columns if col in df.columns]
        
        print(f"Found {len(columns_to_drop)} email columns to remove:")
        for col in columns_to_drop:
            print(f"  - {col}")
        
        # Drop the email columns
        df_cleaned = df.drop(columns=columns_to_drop)
        
        # Determine output filename
        if output_file is None:
            # Add '_no_emails' before the file extension
            base_name = input_file.rsplit('.', 1)[0]
            output_file = f"{base_name}_no_emails.csv"
        
        # Save the cleaned dataframe
        df_cleaned.to_csv(output_file, index=False)
        print(f"\nCleaned file saved to: {output_file}")
        print(f"Original columns: {len(df.columns)}")
        print(f"Remaining columns: {len(df_cleaned.columns)}")
        
        return df_cleaned
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    # Default input file
    input_file = "Targeted-Tech-and-Biotech-Executives-Default-view-export-1751422669424.csv"
    
    # Check if a filename was provided as command line argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # Optional: specify output file as second argument
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Remove email columns
    remove_email_columns(input_file, output_file) 