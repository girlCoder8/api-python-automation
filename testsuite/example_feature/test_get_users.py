from controller.example_feature.users import *
import pytest


@pytest.fixture(scope="function", autouse=True)
def prepare_params(setup):
    routed_path = '/users/weigo'
    api_url = f'{setup.url_prefix}{routed_path}'
    setup.logic_controller = Users(api_url, setup.dataset)
    yield setup


class TestGetUsers:
    def test_rat_get_users(self, setup):
        # Provide Steps
        result = setup.logic_controller.get_users()
        # Provide Verfication
        UsersAssertion.verify_general_response_code_200(result)
