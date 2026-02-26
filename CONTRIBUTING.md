# Contributing to Jellyfin IMDb Ratings

Issues and pull requests are welcome!

## Getting Started

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes
4. Submit a pull request

## Building

```bash
dotnet build --configuration Release
```

## Linting

Run lint checks locally before opening a PR:

```bash
dotnet format whitespace --verify-no-changes
dotnet format style --verify-no-changes --severity warn
```

## Reporting Issues

- Search existing issues before opening a new one
- Include Jellyfin version, plugin version, and relevant logs

## Pull Requests

- Keep changes focused and minimal
- Test against a running Jellyfin instance before submitting
- Describe what your PR changes and why

## LLM Disclosure

This project uses LLM-assisted development (Claude). Contributions generated with AI assistance are welcome, but please review and test all code before submitting.
