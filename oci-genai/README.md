# OCI Generative AI Integration for IronOraClaw

Optional LLM backend using [OCI Generative AI](https://docs.oracle.com/en-us/iaas/Content/generative-ai/home.htm) service. This module provides a local OpenAI-compatible proxy that authenticates with OCI using your existing `~/.oci/config` profile and forwards requests to OCI GenAI.

The default LLM backend (Ollama) remains unchanged. This integration is entirely optional.

## Prerequisites

- Python 3.11+
- `~/.oci/config` configured with valid OCI credentials
- An OCI compartment with Generative AI service enabled

## Quick Start

1. **Install dependencies:**
   ```bash
   cd oci-genai
   pip install -r requirements.txt
   ```

2. **Set required environment variables:**
   ```bash
   export OCI_PROFILE=DEFAULT
   export OCI_REGION=us-chicago-1
   export OCI_COMPARTMENT_ID=ocid1.compartment.oc1..your-compartment-ocid
   ```

3. **Start the proxy:**
   ```bash
   python proxy.py
   # Proxy runs at http://localhost:9999/v1
   ```

4. **Configure IronOraClaw** (`.env` or environment):
   ```bash
   LLM_BACKEND=openai_compatible
   LLM_BASE_URL=http://localhost:9999/v1
   LLM_API_KEY=oci-genai
   LLM_MODEL=meta.llama-3.3-70b-instruct
   ```

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `OCI_PROFILE` | OCI config profile name | `DEFAULT` |
| `OCI_REGION` | OCI region for GenAI endpoint | `us-chicago-1` |
| `OCI_COMPARTMENT_ID` | OCI compartment OCID (required) | -- |
| `OCI_PROXY_PORT` | Local proxy listen port | `9999` |

## Available OCI GenAI Models

| Model ID | Description |
|---|---|
| `meta.llama-3.3-70b-instruct` | Meta Llama 3.3 70B Instruct |
| `xai.grok-3-mini` | xAI Grok 3 Mini |
| `cohere.command-r-plus` | Cohere Command R+ |

Model availability varies by region. Check the [OCI GenAI documentation](https://docs.oracle.com/en-us/iaas/Content/generative-ai/home.htm) for the latest model list and regional availability.

## Architecture

```
IronOraClaw (Rust)
    |
    |  LLM_BACKEND=openai_compatible
    |  LLM_BASE_URL=http://localhost:9999/v1
    v
proxy.py (Python, local)
    |
    |  OCI User Principal Auth (~/.oci/config)
    v
OCI Generative AI Service
```

The proxy translates standard OpenAI Chat Completions API calls into OCI-authenticated requests using the `oci-openai` library. IronOraClaw connects to the proxy as if it were any other OpenAI-compatible endpoint.

## Files

| File | Purpose |
|---|---|
| `oci_client.py` | OCI GenAI client wrapper (sync + async) |
| `proxy.py` | Local OpenAI-compatible proxy server |
| `requirements.txt` | Python dependencies |
