from pymongo import MongoClient
from controller.settings import Settings
from bson.objectid import ObjectId
from typing import Generator, Tuple
import time


class MongoDBManager:
    def __init__(self, settings: Settings):
        # self.namespace = ObjectId(settings.test_settings.account_id)

        if settings.mongodb_server:
            if settings.disable_ssh_tunnel:
                mongo_server_url = settings.mongodb_server
                mongo_port = settings.mongodb_port
            else:
                mongo_server_url = "127.0.0.1"
                mongo_port = settings.ssh_local_bind_port

            self.MONGO_SERVER = MongoClient(f"mongodb://{mongo_server_url}",
                                            username=settings.mongodb_username,
                                            password=settings.mongodb_password,
                                            authSource=settings.mongodb_collection,
                                            port=mongo_port,
                                            authMechanism='SCRAM-SHA-256')

        if settings.mongodb_collection:
            self.DB = self.MONGO_SERVER[settings.mongodb_collection]
            self.accounts = self.DB['accounts']
            self.usdp_supported_ops = self.DB['usdp_supported_ops']
            self.audiences = self.DB['audiences']
            self.exec_instances = self.DB['exec_instances']
            self.account_interests = self.DB['account_interests']
            self.nodes = self.DB['nodes']
            self.unification_spaces = self.DB['unification_spaces']
            self.scenario_templates = self.DB["scenario_templates"]
            self.ai_predictions = self.DB['ai_predictions']

    @staticmethod
    def generate_app_id():
        return ObjectId().__str__()[:-4]

    def get_edp_settings(self, account_id) -> dict:
        return_objects = self.accounts.find({'_id': ObjectId(account_id)})
        account_details = list()
        for account in return_objects:
            account_details.append(account)
        if len(account_details) > 1:
            print("warning: should not exist two account with the same account_id")
        elif len(account_details) == 0:
            print("warning: can not find matched account")
        else:
            return account_details[0]["edp_settings"]

    def get_scenario_based_settings(self, account_id) -> dict:
        return_objects = self.accounts.find({'_id': ObjectId(account_id)})
        account_details = list()
        for account in return_objects:
            account_details.append(account)
        if len(account_details) > 1:
            print("warning: should not exist two account with the same account_id")
        elif len(account_details) == 0:
            print("warning: can not find matched account")
        else:
            return account_details[0]["scenario_based_settings"]

    def get_scenario_templates_types(self, account_id) -> list:
        account_details = self.accounts.find_one({'_id': ObjectId(account_id)})
        return account_details['scenario_templates_types']

    def remove_edp_settings(self, account_id):
        self.accounts.update_one({'_id': ObjectId(account_id)}, {"$set": {"edp_settings": {}}})

    def recover_edp_settings(self, account_id, namespace_id):
        self.accounts.update_one({'_id': ObjectId(account_id)}, {"$set": {"edp_settings": {"namespace_id" : namespace_id}}})

    def recover_purchase_settings(self, account_id, purchase_settings):
        self.accounts.update_one({'_id': ObjectId(account_id)}, {"$set": {"scenario_based_settings": purchase_settings}})

    def remove_scenario_based_settings(self, account_id):
        self.accounts.update_one({'_id': ObjectId(account_id)}, {"$set": {"scenario_based_settings": {}}})

    def get_support_ops(self, value_type) -> list:
        return_objects = self.usdp_supported_ops.find({'query_type': 'event'})

        for support_ops in return_objects:
            if support_ops["value_type"] == value_type.lower():
                return support_ops["supported_ops"]
        return None

    def get_audience_detail(self, audience_id) -> dict:
        return_objects = self.audiences.find_one({'_id': ObjectId(audience_id)})
        return return_objects

    def get_exe_instances(self, audience_id) -> list:
        return_objects = self.exec_instances.find({'doc_id': ObjectId(audience_id)})
        return return_objects

    def delete_audience(self, audience_name) -> dict:
        deleted_at = int(time.time())
        update = {
            '$set': {
                'status': 'DELETED',
                'deleted_at': deleted_at
            },
        }
        self.audiences.update_one({'name': audience_name}, update)

    def get_audience_ids(self, audience_name, status=None) -> list:
        return_objects = self.audiences.find({'name': {'$regex': audience_name}})
        audience_ids = list()
        for audience in return_objects:
            if status is None:
                audience_ids.append(str(audience['_id']))
            else:
                if audience["status"] in status:
                    audience_ids.append(str(audience['_id']))
        return audience_ids

    def get_audience_output_node(self, audience_id) -> list:
        return_objects = self.audiences.find_one({'_id': ObjectId(audience_id)})
        return return_objects['output']

    def get_list_available_interest_schema_by_account_id(self, account_id: str):
        obj_account_id = ObjectId(account_id)
        pipeline = [
          {
            "$match": {
              "account_id": obj_account_id
            }
          },
          {
            "$group": {
              "_id": "$app_id",
              "platforms": {
                "$addToSet": "$platform"
              },
              "versions": {
                "$addToSet": "$version"
              }
            }
          },
          {
            "$project": {
              "_id": 0,
              "appId": "$_id",
              "platforms": 1,
              "versions": 1
            }
          },
          {
            "$sort": {"app_id": 1}
          }
        ]
        return_objects = self.account_interests.aggregate(pipeline)
        records = None
        if return_objects is not None:
            records = []
            for return_object in return_objects:
                records.append(return_object)
        return records

    def get_audience_conditions_fe_by_audience_id(self, audience_id) -> dict:
        return_object = self.audiences.find_one({'_id': ObjectId(audience_id)})
        return return_object['audience_conditions_fe']

    def get_intelligent_matching_by_audience_id(self, audience_id) -> dict:
        return_object = self.audiences.find_one({'_id': ObjectId(audience_id)})
        return return_object['enable_intelligent_matching']

    def get_intelligent_matching_by_account_id(self, account_id) -> dict:
        return_object = self.accounts.find_one({'_id': ObjectId(account_id)})
        return return_object['intelligent_matching_settings']

    def get_interests(self,
                      account_id: str,
                      app_id: str,
                      version: str,
                      platforms: list,
                      topic_name: str) -> Generator:
        obj_account_id = ObjectId(account_id)
        return_objects = self.account_interests.find({'$and': [
            {"account_id": obj_account_id},
            {"app_id": app_id},
            {"version": version},
            {"platform": {"$in": platforms}},
            {"topic_name": topic_name}
        ]})

        if return_objects is not None:
            return (row for row in return_objects)

    def get_node_info_with_id(self, node_id) -> dict:
        return_object = self.nodes.find_one({'_id': ObjectId(node_id)})
        return return_object

    def get_node_info_with_name_and_status(self, node_name, status) -> dict:
        return_objects = self.nodes.find({'node_desc': {'$regex': node_name}})
        node_list = list()
        for node in return_objects:
            if node['status'] in status:
                node_list.append(node)
        if len(node_list) == 1:
            return node_list[0]
        return node_list

    def get_unification_space_with_id(self, pk) -> dict:
        return_object = self.unification_spaces.find_one({'_id': ObjectId(pk)})
        return return_object

    def get_unification_space_with_name(self, space_name) -> dict:
        return_object = self.unification_spaces.find_one({'name': space_name})
        return return_object

    def get_scenario_templates_ids_by_name(self, names: list):
        return_objects = self.scenario_templates.find({"module": {"$in": names}})
        ids = [str(obj["_id"]) for obj in return_objects]
        return ids

    def get_ai_predictions_by_account_id(self, account_id: str):
        obj_account_id = ObjectId(account_id)
        return_objects = self.ai_predictions.find({'$and': [
            {"account_id": obj_account_id},
            {'status': {"$ne": "DELETED"}}]
        })
        return return_objects

    def get_prediction_tags_by_prediction_id(self, prediction_id: str):
        obj_prediction_id = ObjectId(prediction_id)
        return_object = self.ai_predictions.find_one({'_id': obj_prediction_id})
        return return_object["tags"] if "tags" in return_object else None

    def get_ai_predictions_with_fb_campaign_settings_by_account_id(self, account_id: str):
        obj_account_id = ObjectId(account_id)
        return_objects = self.ai_predictions.find({'$and': [
            {"account_id": obj_account_id},
            {'status': {"$ne": "DELETED"}},
            {'user_acquisition_from_ads_info.fb_campaign_settings': {"$exists": True}}
        ]
        })
        return return_objects

    def get_ai_prediction_id_with_name_and_status(self, prediction_name, status: list = None) -> list:
        if status is None:
            _status = ["MODEL_READY"]
        else:
            _status = status
        return_objects = self.ai_predictions.find({'name': {'$regex': prediction_name}})
        ai_prediction_list = []
        for ai_prediction in return_objects:
            if ai_prediction['status'] in _status:
                ai_prediction_list.append(str(ai_prediction['_id']))
        return ai_prediction_list

    def get_ai_prediction_info_with_id(self, ai_prediction_id) -> dict:
        return_object = self.ai_predictions.find_one({'_id': ObjectId(ai_prediction_id)})
        return return_object

    def get_node_info_with_pseudo_group_id(self, pseudo_group_id) -> dict:
        return_object = self.nodes.find_one({'pseudo_group_id': ObjectId(pseudo_group_id)})
        return return_object
