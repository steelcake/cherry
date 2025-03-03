import logging

import pytest


# Configure logging for tests
@pytest.fixture(autouse=True)
def configure_logging():
    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    yield
