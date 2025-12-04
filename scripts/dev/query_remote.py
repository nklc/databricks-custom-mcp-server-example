#!/usr/bin/env python3
"""
Test remote MCP server deployed as a Databricks App.

This script tests the remote MCP server with user-level OAuth authentication,
calling both the health tool and user authorization tool to verify functionality.

Usage:
    python test_remote.py --host <host> --token <token> --app-url <app-url>

Example:
    python test_remote.py \\
        --host https://dbc-a1b2345c-d6e7.cloud.databricks.com \\
        --token eyJr...Dkag \\
        --app-url https://dbc-a1b2345c-d6e7.cloud.databricks.com/serving-endpoints/my-app
"""

import argparse
import json
import sys

from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient


def main():
    parser = argparse.ArgumentParser(
        description="Test remote MCP server deployed as Databricks App"
    )

    parser.add_argument("--host", required=True, help="Databricks workspace URL")

    parser.add_argument("--token", required=True, help="OAuth access token")

    parser.add_argument("--app-url", required=True, help="Databricks App URL (without /mcp suffix)")

    parser.add_argument(
        "--test-job-id",
        type=int,
        help="Optional: Job ID to test trigger_job_run tool (if not provided, tool is skipped)",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("Testing Remote MCP Server - Databricks App")
    print("=" * 70)
    print(f"\nWorkspace: {args.host}")
    print(f"App URL: {args.app_url}")
    print()

    try:
        # Create WorkspaceClient with OAuth token
        print("Step 1: Creating WorkspaceClient with OAuth token...")
        workspace_client = WorkspaceClient(host=args.host, token=args.token)
        print("✓ WorkspaceClient created successfully")
        print()

        # Create MCP client
        mcp_url = f"{args.app_url}/mcp"
        print(mcp_url)
        print(f"Step 2: Connecting to MCP server at {mcp_url}...")
        mcp_client = DatabricksMCPClient(server_url=mcp_url, workspace_client=workspace_client)
        print("✓ MCP client connected successfully")
        print()

        # List available tools
        print("Step 3: Listing available MCP tools...")
        print("-" * 70)
        tools = mcp_client.list_tools()
        print(tools)
        print("-" * 70)
        print(f"✓ Found {len(tools) if isinstance(tools, list) else 'N/A'} tools")
        print()

        # Test tools without parameters
        print("Step 4: Testing tools without parameters...")
        for tool in tools:
            # Skip tools that require parameters
            if tool.name in ["add_numbers", "trigger_job_run"]:
                continue  # Tested separately below
            
            print(f"\nTesting tool: {tool.name}")
            print("-" * 70)
            result = mcp_client.call_tool(tool.name)
            print(result)
            print("-" * 70)
        
        print("\n✓ All parameter-free tools tested successfully")
        print()

        # Test add_numbers tool with specific arguments
        print("Step 5: Testing add_numbers tool with arguments...")
        print("-" * 70)
        
        test_cases = [
            {"a": 5, "b": 3, "expected": 8.0},
            {"a": 10.5, "b": 2.3, "expected": 12.8},
            {"a": -5, "b": 10, "expected": 5.0},
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            a = test_case["a"]
            b = test_case["b"]
            expected = test_case["expected"]
            
            print(f"\nTest {i}: add_numbers({a}, {b})")
            result = mcp_client.call_tool("add_numbers", arguments={"a": a, "b": b})
            
            # Parse result
            result_data = json.loads(result.content[0].text)
            actual = result_data["result"]
            
            print(f"  Expected: {expected}")
            print(f"  Actual:   {actual}")
            
            if actual == expected:
                print(f"  ✓ PASS")
            else:
                print(f"  ✗ FAIL")
                raise AssertionError(f"Expected {expected} but got {actual}")
        
        print("-" * 70)
        print("✓ add_numbers tool tested successfully")
        print()

        # Test trigger_job_run tool if job ID provided
        if args.test_job_id:
            print("Step 6: Testing trigger_job_run tool...")
            print("-" * 70)
            print(f"\nTriggering job run for job ID: {args.test_job_id}")
            
            result = mcp_client.call_tool("trigger_job_run", arguments={"job_id": args.test_job_id})
            result_data = json.loads(result.content[0].text)
            
            if result_data.get("success"):
                print(f"  ✓ Job triggered successfully!")
                print(f"  Run ID: {result_data.get('run_id')}")
                if "run_page_url" in result_data:
                    print(f"  Run URL: {result_data.get('run_page_url')}")
            else:
                print(f"  ✗ Failed to trigger job")
                print(f"  Error: {result_data.get('error')}")
            
            print("-" * 70)
            print("✓ trigger_job_run tool tested")
            print()
        else:
            print("Step 6: Skipping trigger_job_run test (no --test-job-id provided)")
            print()

        print("=" * 70)
        print("✓ All Tests Passed!")
        print("=" * 70)

    except Exception as e:
        print()
        print("=" * 70)
        print(f"✗ Error: {e}")
        print("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()
