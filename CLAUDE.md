# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Syncbit is a high-performance, peer-assisted data distribution engine designed for distributing large datasets (ML models, container layers) across clustered environments. It solves the problem of efficiently distributing multi-GB/TB files to specific nodes while minimizing redundant downloads from external sources.

## Architecture

The system consists of:
- **syncbitd**: Daemon that runs in Controller mode (central scheduler) or Agent mode (node daemon)
- **syncbit**: CLI client for submitting jobs and monitoring status
- **Controller**: Tracks file availability, schedules download tasks, manages agent health
- **Agents**: Manage local cache, execute downloads, handle P2P transfers between nodes
- **Providers**: Pluggable sources - HuggingFace, S3, HTTP, and peer-to-peer

## Common Development Commands

```bash
# Build everything
task

# Development tasks
task fmt          # Format code
task lint         # Lint code (go vet + gopls)
task test         # Run all tests
task test-working # Test stable components only
task clean        # Clean build artifacts

# Run services
task controller -- --config config.controller.yaml
task agent -- --config config.agent1.yaml
task cli -- <command>

# E2E testing
./run_p2p_test.sh      # Quick P2P replication test
./test_e2e.sh          # Comprehensive end-to-end test
./monitor_logs.sh all  # Real-time log monitoring
```

## Key Design Patterns

1. **Block-based caching**: Files are split into blocks for efficient caching and P2P transfer
2. **Provider abstraction**: All data sources implement a common interface
3. **Job-based scheduling**: Downloads are tracked as jobs with distribution strategies
4. **Heartbeat-based health**: Agents periodically report health to controller
5. **Configuration**: YAML-based with named provider support

## Important Context

- The codebase is transitioning away from block-level peer code (see TODO.md)
- Cache system uses RAM + disk with LRU eviction
- All file paths should use `filepath.Join`, URLs should use `url.JoinPath`
- Dataset and file names need proper URL encoding for API endpoints
- Replication factor support ensures high availability

## Testing Approach

- Unit tests: Standard Go testing in `*_test.go` files
- Integration tests: E2E scripts that spin up controller + agents
- Use `task test-working` to test stable components during development
- Monitor logs with `./monitor_logs.sh` during debugging

## Current Development Focus

Per TODO.md, cleanup work includes:
- URL encoding for dataset/file lookups
- Removing legacy block-level peer code
- Modernizing code with gopls recommendations
- Implementing cache corruption recovery on startup