1. Create a virtual environment and activate it by running:

```shell
python3 -m venv .venv
source .venv/bin/activate
```
2. Install packages

```shell
uv sync
```

3. To log in to Google Cloud

```shell
   gcloud auth application-default login   
```