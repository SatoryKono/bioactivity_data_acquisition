"""CLI entrypoint for executing the UniProt pipeline."""

from scripts import create_pipeline_app

app = create_pipeline_app(
    "uniprot",
    "Run UniProt pipeline to enrich accessions with UniProt metadata",
)


if __name__ == "__main__":
    app()
