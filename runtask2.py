"""
Automated setup script for Task 2
Runs all steps to set up the data warehouse
"""

import os
import sys
import subprocess
from pathlib import Path


def run_command(cmd, description, check=True):
    """Run a shell command and handle errors"""
    print(f"\n{'='*60}")
    print(f"▶ {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
        else:
            if result.stderr:
                print(f"⚠️  Warning: {result.stderr}")
        
        return result.returncode == 0
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        return False


def check_prerequisites():
    """Check if required tools are installed"""
    print("\n" + "="*60)
    print("CHECKING PREREQUISITES")
    print("="*60)
    
    checks = {
        'Python': 'python --version',
        'pip': 'pip --version',
        'PostgreSQL': 'psql --version',
        'dbt': 'dbt --version',
    }
    
    all_ok = True
    for tool, cmd in checks.items():
        result = subprocess.run(cmd, shell=True, capture_output=True)
        if result.returncode == 0:
            print(f"✅ {tool} is installed")
        else:
            print(f"❌ {tool} is NOT installed")
            all_ok = False
    
    return all_ok


def main():
    """Main execution"""
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║                                                            ║
    ║         TASK 2: DATA WAREHOUSE SETUP AUTOMATION            ║
    ║                                                            ║
    ║  This script will:                                         ║
    ║  1. Check prerequisites                                    ║
    ║  2. Load raw data to PostgreSQL                            ║
    ║  3. Install dbt packages                                   ║
    ║  4. Run dbt models                                         ║
    ║  5. Run dbt tests                                          ║
    ║  6. Generate documentation                                 ║
    ║                                                            ║
    ╚════════════════════════════════════════════════════════════╝
    """)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Some prerequisites are missing. Please install them first.")
        print("\nInstallation commands:")
        print("  pip install dbt-core dbt-postgres psycopg2-binary")
        print("  # Install PostgreSQL from https://www.postgresql.org/download/")
        return
    
    # Confirm to proceed
    response = input("\nProceed with setup? (y/n): ")
    if response.lower() != 'y':
        print("Setup cancelled.")
        return
    
    # Change to project directory
    project_root = Path(__file__).parent
    os.chdir(project_root)
    
    # Step 1: Load raw data to PostgreSQL
    if not run_command(
        'python src/load_to_postgres.py',
        'Loading raw data to PostgreSQL'
    ):
        print("\n⚠️  Data loading failed. Check your database configuration.")
        return
    
    # Step 2: Change to dbt project directory
    dbt_project_dir = project_root / 'medical_warehouse'
    if not dbt_project_dir.exists():
        print(f"\n❌ dbt project directory not found: {dbt_project_dir}")
        print("Please create the dbt project first.")
        return
    
    os.chdir(dbt_project_dir)
    
    # Step 3: Test dbt connection
    if not run_command(
        'dbt debug',
        'Testing dbt connection',
        check=False
    ):
        print("\n⚠️  dbt connection test failed. Check ~/.dbt/profiles.yml")
        print("Make sure your database credentials are correct.")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return
    
    # Step 4: Install dbt packages
    run_command(
        'dbt deps',
        'Installing dbt packages'
    )
    
    # Step 5: Run dbt models
    if not run_command(
        'dbt run',
        'Running dbt transformations'
    ):
        print("\n❌ dbt run failed. Check the error messages above.")
        return
    
    # Step 6: Run dbt tests
    if not run_command(
        'dbt test',
        'Running dbt tests',
        check=False
    ):
        print("\n⚠️  Some tests failed. Review the failures above.")
        print("You can check failed tests in the test_failures schema.")
    
    # Step 7: Generate documentation
    run_command(
        'dbt docs generate',
        'Generating dbt documentation'
    )
    
    # Final summary
    print("\n" + "="*60)
    print("SETUP COMPLETE!")
    print("="*60)
    print("\n✅ Your data warehouse is ready!")
    print("\nNext steps:")
    print("  1. View documentation: dbt docs serve")
    print("  2. Query your data: psql -U postgres -d medical_warehouse")
    print("  3. Explore models in: models/marts/")
    print("\nSample query:")
    print("  SELECT * FROM marts.fct_messages LIMIT 10;")
    print("\n" + "="*60)


if __name__ == '__main__':
    main()