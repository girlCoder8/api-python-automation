import pytest
import logging
from controller.settings import Settings


def pytest_addoption(parser):
    parser.addoption('--env', action='store',
                     help='setup environment; STAGING, PROD')
    parser.addoption('--dataset', action='store',
                     help='setup name of test data set (ex: test_data_set_1)')
    parser.addoption('--disable_ssh_tunnel', action='store_true', default=False,
                     help='Setup SSH Tunnel to connect with MongoDB (ex: True=1 or False=0)')
    parser.addoption('--settings_file', action='store', default=None,
                     help='setup settings data (ex: AIXON , EDP ; Default is AIXON)')
    parser.addoption("--api_version", action="store", metavar="api_version", default=None,
                     help="only run tests matching the api version as api_version.")

def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line("markers",
                            "version(number): mark test to run only on named version")


@pytest.fixture(scope="session", autouse=True)
def setup(request):
    setup = Settings(request)
    # If there are any plugin object would be shared to use
    # You can initiate here
    # Ex: setup.mongodb = MongoDBManager(setup)
    yield setup


def pytest_runtest_setup(item):
    version_marker = item.get_closest_marker("version")
    assigned_version = item.config.getoption("--api_version")
    if version_marker:
        version = version_marker.args[0]
        if not item.config.getoption("--api_version"):
            logging.warning("Not assigned api_version argument, but test case requires version as {}"
                            .format(version))
        elif version != assigned_version:
            pytest.skip("test requires running on api version {}".format(version))
    else:
        logging.warning("Not found marker of the api version")
        logging.warning("Run test case by assigned api version as {}".format(
            assigned_version if assigned_version else Settings.DEFAULT_API_VERSION))