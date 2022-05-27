from controller.api_util.base_request import Base, BaseAssertion


class Users(Base):
    def __init__(self, url, data_set):
        Base.__init__(self)
        self.data_set = data_set
        self.url = url

    def get_users(self):
        res = self.send_request(
            Base.RequestMethod.GET,
            custom_url=self.url
        )
        return res


class UsersAssertion(BaseAssertion):
    @classmethod
    def verify_specific_results(cls, res: Base.ResponseObject):
        # Here to verify specific results from Response Object
       pass