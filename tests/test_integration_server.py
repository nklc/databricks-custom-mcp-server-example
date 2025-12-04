import os
import shlex
import signal
import socket
import subprocess
import time
from contextlib import closing
import json
import pytest
import requests
from databricks_mcp import DatabricksMCPClient


def _find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server_startup(url: str, timeout: int = 10):
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=1)
            if 200 <= response.status_code < 400:
                return response
        except Exception as e:
            last_exc = e
        time.sleep(0.1)
    if last_exc:
        raise last_exc

    raise TimeoutError(f"Server at {url} did not respond in {timeout} seconds")


@pytest.fixture(scope="session")
def run_mcp_server():
    host = "127.0.0.1"
    port = _find_free_port()
    url = f"http://{host}:{port}"
    cmd = shlex.split(f"uv run custom-mcp-server --port {port}")

    # Start the process
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        # Start a new process group so we can kill children on teardown
        preexec_fn=os.setsid,
        creationflags=0,
    )

    try:
        _wait_for_server_startup(url)
    except Exception as e:
        proc.terminate()
        raise e

    yield url

    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=10)
    except Exception:
        os.killpg(proc.pid, signal.SIGKILL)
    finally:
        proc.wait(timeout=5)


# Test List Tools runs without errors
def test_list_tools(run_mcp_server):
    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    tools = mcp_client.list_tools()
    for tool in tools:
        print(f"name: {tool.name}")
        print(f"description: {tool.description}")


# Test Call Tools runs without errors
def test_call_tools(run_mcp_server):
    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    tools = mcp_client.list_tools()
    for tool in tools:
        # Skip tools that require parameters if they don't have defaults
        if tool.name in ["add_numbers", "trigger_job_run"]:
            continue  # Tested separately
        result = mcp_client.call_tool(tool.name)
        assert result is not None


# Test add_numbers tool with actual arguments
def test_add_numbers(run_mcp_server):
    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    
    # Test with integers
    result = mcp_client.call_tool("add_numbers", arguments={"a": 5, "b": 3})
    assert result is not None
    assert result.content[0].text is not None
    result_data = json.loads(result.content[0].text)
    assert result_data["result"] == 8.0
    assert result_data["a"] == 5.0
    assert result_data["b"] == 3.0
    
    # Test with floats
    result = mcp_client.call_tool("add_numbers", arguments={"a": 10.5, "b": 2.3})
    assert result is not None
    assert result.content[0].text is not None
    result_data = json.loads(result.content[0].text)
    assert result_data["result"] == 12.8
    assert result_data["a"] == 10.5
    assert result_data["b"] == 2.3
    
    # Test with negative numbers
    result = mcp_client.call_tool("add_numbers", arguments={"a": -5, "b": 10})
    assert result is not None
    assert result.content[0].text is not None
    result_data = json.loads(result.content[0].text)
    assert result_data["result"] == 5.0


# Test trigger_job_run tool with invalid job ID (error handling)
def test_trigger_job_run_error_handling(run_mcp_server):
    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    
    # Test with an invalid job ID (should return error gracefully)
    # Using 999999999 as a job ID that likely doesn't exist
    result = mcp_client.call_tool("trigger_job_run", arguments={"job_id": 999999999})
    assert result is not None
    assert result.content[0].text is not None
    result_data = json.loads(result.content[0].text)
    
    # Should have success=False and an error message
    assert result_data["success"] is False
    assert "error" in result_data
    assert result_data["job_id"] == 999999999
    print(f"Error message (expected): {result_data['error']}")
