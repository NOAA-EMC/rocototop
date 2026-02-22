# Instructions for AI Agents

When working on this repository, please follow these guidelines:

1. **Textual Best Practices**: We use the Textual framework for the TUI. Follow Textual's recommended patterns for composing widgets and handling events.
2. **Performance**: Ensure that heavy I/O operations (like parsing XML or querying SQLite) are performed in background workers using `@work(thread=True)`.
3. **Structure**: Maintain the `src/` layout. Core logic should be in `src/rocotoviewer/`.
4. **Documentation**: Update docstrings and README.md when changing features or CLI arguments.
