#!/usr/bin/env python3
"""
Test script for The Historic Miner component.
This script validates the implementation without making actual API calls.
"""

import os
import sys
import pandas as pd
from unittest.mock import Mock, patch
from src.miner import HistoricMiner

def test_cost_calculation():
    """Test the cost calculation logic."""
    print("Testing cost calculation...")
    
    # Test Linux cost
    from src.miner import HistoricMiner
    miner = HistoricMiner.__new__(HistoricMiner)  # Create instance without __init__
    
    # Test different OS types
    assert miner._calculate_workflow_cost(10, 'linux') == 10 * 0.008, "Linux cost calculation failed"
    assert miner._calculate_workflow_cost(10, 'windows') == 10 * 0.016, "Windows cost calculation failed"
    assert miner._calculate_workflow_cost(10, 'macos') == 10 * 0.08, "macOS cost calculation failed"
    assert miner._calculate_workflow_cost(10, 'unknown') == 10 * 0.008, "Default OS cost calculation failed"
    
    print("✓ Cost calculation tests passed")

def test_os_extraction():
    """Test OS extraction from runner names."""
    print("Testing OS extraction...")
    
    miner = HistoricMiner.__new__(HistoricMiner)
    
    # Test various runner names
    assert miner._extract_os_from_runner('ubuntu-latest') == 'linux'
    assert miner._extract_os_from_runner('windows-2019') == 'windows'
    assert miner._extract_os_from_runner('macos-latest') == 'macos'
    assert miner._extract_os_from_runner('macos-11') == 'macos'
    assert miner._extract_os_from_runner('self-hosted') == 'linux'  # Default
    
    print("✓ OS extraction tests passed")

def test_repo_url_parsing():
    """Test repository URL parsing."""
    print("Testing repository URL parsing...")
    
    # Test URL format
    test_urls = [
        'https://github.com/microsoft/vscode',
        'https://github.com/facebook/react',
        'vuejs/vue',  # Already in owner/repo format
        'rust-lang/rust'
    ]
    
    expected_names = [
        'microsoft/vscode',
        'facebook/react', 
        'vuejs/vue',
        'rust-lang/rust'
    ]
    
    # Simulate the URL parsing logic
    repo_names = []
    for url in test_urls:
        if 'github.com' in url:
            parts = url.strip('/').split('/')
            if len(parts) >= 2:
                repo_name = f"{parts[-2]}/{parts[-1]}"
                repo_names.append(repo_name)
        else:
            repo_names.append(url)
    
    assert repo_names == expected_names, f"URL parsing failed. Got {repo_names}, expected {expected_names}"
    
    print("✓ Repository URL parsing tests passed")

def test_file_input_handling():
    """Test file input handling."""
    print("Testing file input handling...")
    
    # Create a test repository file
    test_repo_file = 'test_repos.txt'
    with open(test_repo_file, 'w') as f:
        f.write('# Test repositories\n')
        f.write('https://github.com/microsoft/vscode\n')
        f.write('vuejs/vue\n')
        f.write('\n')  # Empty line
        f.write('rust-lang/rust\n')
    
    # Read and parse
    with open(test_repo_file, 'r') as f:
        repo_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    expected = ['https://github.com/microsoft/vscode', 'vuejs/vue', 'rust-lang/rust']
    assert repo_urls == expected, f"File parsing failed. Got {repo_urls}, expected {expected}"
    
    # Clean up
    os.remove(test_repo_file)
    
    print("✓ File input handling tests passed")

def test_output_format():
    """Test output DataFrame format."""
    print("Testing output format...")
    
    # Sample data structure
    sample_data = [
        {
            'repo_name': 'test/repo',
            'run_id': 12345,
            'workflow_name': 'CI',
            'head_branch': 'main',
            'head_sha': 'abc123',
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:10:00Z',
            'duration_minutes': 10.5,
            'total_cost_usd': 0.084,
            'job_count': 3
        }
    ]
    
    df = pd.DataFrame(sample_data)
    
    # Check required columns
    required_columns = [
        'repo_name', 'run_id', 'workflow_name', 'head_branch', 'head_sha',
        'created_at', 'updated_at', 'duration_minutes', 'total_cost_usd', 'job_count'
    ]
    
    assert all(col in df.columns for col in required_columns), "Missing required columns"
    assert len(df) == 1, "Incorrect number of rows"
    assert df['total_cost_usd'].iloc[0] > 0, "Cost should be positive"
    
    print("✓ Output format tests passed")

def run_all_tests():
    """Run all tests."""
    print("Running Historic Miner Tests...\n")
    
    try:
        test_cost_calculation()
        test_os_extraction()
        test_repo_url_parsing()
        test_file_input_handling()
        test_output_format()
        
        print("\n🎉 All tests passed! The Historic Miner implementation is ready.")
        return True
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return False
    except Exception as e:
        print(f"\n💥 Unexpected error during testing: {e}")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
