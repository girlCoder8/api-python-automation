from .common_imports import *
import uuid
import requests
from requests.models import Response


class Base(object):
    class ResponseObject(object):
        def __init__(self, response: Response):
            self.status_code = response.status_code
            self.content = response.content
            self.text = response.text
            try:
                self.json = response.json()
            except Exception as e:
                self.json = None
                logger.warning(e)
            self.header = response.headers
            self.url = response.url

    class RequestMethod(str, Enum):
        GET = "GET"
        POST = "POST"
        PUT = "PUT"
        DELETE = "DELETE"
        PATCH = "PATCH"

    def __init__(self) -> None:
        pass

    @staticmethod
    def gen_unique_str():
        id = uuid.uuid4()
        return id

    def send_request(self,
                     method: RequestMethod = RequestMethod.GET,
                     payload=None,
                     chunk_size: int = 0,
                     cookies=None,
                     custom_url: str = None,
                     headers=None,
                     files: list = None
                     ) -> ResponseObject:

        _payload = None

        if files:
            _payload = {}

        if custom_url is None:
            logging.warning("should provide url when sending request")

        if payload is None:
            logging.warning("should provide payload when sending request")
        else:
            _payload = payload

        if headers is None:
            _headers = {"Content-Type": "application/json"}
        else:
            _headers = headers

        # new request session
        res = None
        session_res = requests.session()

        if method is self.RequestMethod.GET:
            res = session_res.get(custom_url, headers=_headers, cookies=cookies, stream=True)
        elif method is self.RequestMethod.POST:
            res = session_res.post(custom_url, headers=_headers, cookies=cookies, stream=True, json=_payload, files=files)
        elif method is self.RequestMethod.PUT:
            res = session_res.put(custom_url, headers=_headers, cookies=cookies, stream=True, json=_payload, files=files)
        elif method is self.RequestMethod.DELETE:
            res = session_res.delete(custom_url, headers=_headers, cookies=cookies, stream=True)
        elif method is self.RequestMethod.PATCH:
            res = session_res.patch(custom_url, headers=_headers, cookies=cookies, stream=True, json=_payload, files=files)
        else:
            res = session_res.put(custom_url, headers=_headers, cookies=cookies, stream=True, data=_payload, files=files)

        res_obj = self.ResponseObject(res)

        # Formalize output logs for all API tests
        logger.info("\n=============URL=================\n")
        logger.info(res_obj.url)
        logger.info("\n============Payload==============\n")
        logger.info(payload)
        logger.info("\n============Response=============\n")
        if len(res.content) < 10000:
            logger.info(res_obj.text)
        else:
            logger.info("Too large response body, skipped to print.")
        logger.info("\n=================================\n")

        return res_obj


class BaseAssertion:
    @classmethod
    def log_assert(cls, func, messages):
        if not func:
            logging.error(messages)
        assert func, messages

    @classmethod
    def verify_general_response_code_200(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code == 200, "Assertion Failure, The status code is not 200, body: {}".format(res.text))

    @classmethod
    def verify_response_code_with_201(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code == 201,
                       "Assertion Failure, The status code is not 201, body: {}".format(res.text))

    @classmethod
    def verify_response_code_with_202(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code == 202, "Assertion Failure, The status code is not 202, body: {}".format(res.text))

    @classmethod
    def verify_response_code_with_204(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code == 204,
                       "Assertion Failure, The status code is not 204, body: {}".format(res.text))

    @classmethod
    def verify_general_forbidden_response_code(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code == 403,
                       "Assertion Failure, The status code is not 403, body: {}".format(res.text))

    @classmethod
    def verify_response_code_with_404(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code == 404,
                       "Assertion Failure, The status code is not 404, body: {}".format(res.text))

    @classmethod
    def verify_general_bad_request(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code != 400,
                       "Assertion Failure, The status code is not 400, body: {}".format(res.text))

    @classmethod
    def verify_general_bad_request_with_403(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code != 403,
                       "Assertion Failure, The status code is not 403, body: {}".format(res.text))

    @classmethod
    def verify_general_bad_request_with_405(cls, res: Base.ResponseObject):
        cls.log_assert(res.status_code != 405,
                       "Assertion Failure, The status code is not 405, body: {}".format(res.text))
