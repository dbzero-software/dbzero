# Claude Code Docker Image

This image is based on `docker/Dockerfile-python-312`, but adds the extra tooling needed for interactive development with Claude Code inside the container shell.

## Build

```bash
docker build -f docker/Dockerfile-claude -t dbzero-claude .
```

## Run

Mount the repository so the shell works on your current checkout:

```bash
docker run --rm -it \
  -v "$PWD":/usr/src/dbzero \
  dbzero-claude
```

To persist Claude Code login and settings across container runs, also mount your Claude config directory:

```bash
docker run --rm -it \
  -v "$PWD":/usr/src/dbzero \
  -v "$HOME/.claude":/home/claude/.claude \
  dbzero-claude
```

## Authentication Options

Inside the container shell, you can authenticate in either of these ways:

1. Run `claude` and complete the browser-based login flow.
2. Pass `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, or `CLAUDE_CODE_OAUTH_TOKEN` into `docker run` if you already use non-interactive credentials.

Example:

```bash
docker run --rm -it \
  -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  -v "$PWD":/usr/src/dbzero \
  dbzero-claude
```

## Notes

- The image seeds `~/.claude/settings.json` with a minimal default configuration.
- `CLAUDE_CONFIG_DIR` is exported automatically for interactive bash shells.
- The container runs as a non-root `claude` user so `claude --dangerously-skip-permissions` works as expected.
- The repository `scripts/build.sh` and `dbzero/build_package.sh` are left untouched; use `dbzero-build` and `dbzero-build-package` inside the container for the non-root workflow.
- `claude --version` is executed during the image build to verify the installation.