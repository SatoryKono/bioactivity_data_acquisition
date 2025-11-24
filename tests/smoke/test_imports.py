def test_import_bioetl():
    import bioetl  # noqa: F401
    import bioetl.cli.cli_app  # noqa: F401

    assert True
