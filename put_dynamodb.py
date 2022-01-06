import json
import time

import boto3
import botocore
from datetime import datetime


class DynamoDB:
    unused_rule_table_name = 'unusedSecurityGroupRule'
    unused_rule_partition_key = 'SGRuleId'
    sort_key = ''
    not_assigned_sg_table_name = 'notAssignedSecurityGroup'
    not_assigned_sg_partition_key = 'SGId'
    dynamodb_client = boto3.client('dynamodb')

    def _describe_table(self, table_name):
        try:
            return self.dynamodb_client.describe_table(
            TableName=table_name
            )['Table']
        except self.dynamodb_client.exceptions.ResourceNotFoundException as e:
            print(e)


    def create_unused_rule_table(self):
        response = self._describe_table(self.unused_rule_table_name + self.get_datetime())

        if not response:
            self.dynamodb_client.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': self.unused_rule_partition_key,
                        'AttributeType': 'S'
                    }
                ],
                TableName=self.unused_rule_table_name + self.get_datetime(),
                KeySchema=[
                    {
                        'AttributeName': self.unused_rule_partition_key,
                        'KeyType': 'HASH'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
        time.sleep(20)

    def create_not_assigned_table(self):
        response = self._describe_table(self.not_assigned_sg_table_name + self.get_datetime())

        if not response:
            self.dynamodb_client.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': self.not_assigned_sg_partition_key,
                        'AttributeType': 'S'
                    }
                ],
                TableName=self.not_assigned_sg_table_name + self.get_datetime(),
                KeySchema=[
                    {
                        'AttributeName': self.not_assigned_sg_partition_key,
                        'KeyType': 'HASH'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
        time.sleep(20)

    def put_item(self, table_name, item):
        self.dynamodb_client.put_item(
            TableName=table_name,
            Item=item
        )

    def get_datetime(self):
        d = datetime.now()
        return '_' + str(d.year) + str(d.month) + str(d.day) + '_' + str(d.hour)


if __name__ == '__main__':

    dynamodb = DynamoDB()
    dynamodb.create_unused_rule_table()
    dynamodb.create_not_assigned_table()
    d = dynamodb.get_datetime()
    pass
