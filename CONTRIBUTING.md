# Contributing to simpl.E

Thanks for your interest in simpl.E. This is a short stub — it will grow as the
project does. The governing principle is in the
[design handbook](./docs/00-vision-and-naming.md): **keep it as simple as
possible.**

## Dev setup

simpl.E targets Python 3.11+. Clone the repo, then create a virtual environment
and install the package in editable mode with the `dev` extras:

```sh
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run the tests

```sh
pytest -q
```

## Run the linter and type checker

```sh
ruff check .
mypy simple_e
```

Please make sure `pytest`, `ruff check`, and `mypy` all pass before opening a
pull request.
