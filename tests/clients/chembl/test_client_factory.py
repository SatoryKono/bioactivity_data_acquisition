from bioetl.clients.chembl.factory import ChemblClientFactory
from bioetl.pipelines.chembl.common.descriptor_factory import ChemblDescriptorFactory


def test_factory_builds_descriptor_with_configured_fetcher():
    config = {
        "sources": {
            "chembl": {
                "activity_fetcher": lambda batch: [
                    {"chembl_id": chembl_id, "value": 1} for chembl_id in (batch or [])
                ]
            }
        }
    }

    factory = ChemblClientFactory(config)

    descriptor_factory = factory.create("activity")

    assert isinstance(descriptor_factory, ChemblDescriptorFactory)

    descriptor = descriptor_factory.build("activity")
    context = descriptor.build_context(None)
    fetcher = descriptor.fetcher_factory(context)

    assert callable(fetcher)
    assert fetcher(["CHEMBL1"]) == [
        {"chembl_id": "CHEMBL1", "value": 1}
    ]
