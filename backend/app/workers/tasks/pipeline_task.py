from celery import chain, signature

def run_pipeline_chain(raw_uri: str, source: str, config=None, config_uri=None):
    return chain(
        signature("clean_data", args=(raw_uri, source, config, config_uri)),
        signature("build_features", kwargs={"source": source}),
    ).apply_async()
