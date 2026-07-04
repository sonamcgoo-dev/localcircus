#!/usr/bin/env python3
"""
RITUAL Marketplace - Quick Start Script
Runs the integrated marketplace with all wiring active.
"""
import subprocess
import sys
import os

def main():
    print("🚀 Starting RITUAL Marketplace...")
    print()
    print("Integrated System Features:")
    print("  ✓ Marketplace → /registry/search (live query)")
    print("  ✓ Run button → /zoo/run (streaming pipeline)")
    print("  ✓ Tag-indexed model registry")
    print("  ✓ Runtime scores and metrics")
    print("  ✓ Streaming execution (SSE)")
    print("  ✓ Model caching layer")
    print()
    
    # Install dependencies
    print("📦 Installing dependencies...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"])
    print()
    
    # Start server
    print("🌐 Starting server on http://localhost:8000")
    print()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    subprocess.run([sys.executable, "-m", "uvicorn", "src.backend.server:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--reload"])

if __name__ == "__main__":
    main()
