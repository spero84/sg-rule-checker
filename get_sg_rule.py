import json
import boto3

class EC2SecurityGroup:
    security_groups = {}
    default = {}        # default sg
    sg_with_ec2 = {}    # sg in use, if list is empty, not in use
    all_ec2 = {}
    sgr_list = []
    ec2_client = boto3.client('ec2')
    vpc_id_list = []

    def __init__(self, vpc_id_list):
        self.vpc_id_list = vpc_id_list

    # def retrieve_sg_rules(security_info):
    def get_security_resources(self):
        for sg in self.ec2_client.describe_security_groups(
                Filters=[
                    {
                        'Name': 'vpc-id',
                        'Values': self.vpc_id_list
                    },
                ]
        )['SecurityGroups']:
            if sg['GroupName'] == 'default':
                self.default[sg['GroupId']] = 'default security group'
            self.sg_with_ec2[sg['GroupId']] = []
            self.security_groups[sg['GroupId']] = sg
    
        for reservation in self.ec2_client.describe_instances(
                Filters=[
                    {
                        'Name': 'vpc-id',
                        'Values': self.vpc_id_list
                    },
                ]
        )['Reservations']:
            for instance in reservation['Instances']:
                for sg in instance.get('SecurityGroups', []):
                    self.sg_with_ec2[sg['GroupId']].append({"ec2": instance['InstanceId'],
                                  "interface_id": [x['NetworkInterfaceId'] for x in instance['NetworkInterfaces']]})
                self.all_ec2[instance['InstanceId']] = instance

    def describe_security_group_rules(self, group_ids):
        self.sgr_list = self.ec2_client.describe_security_group_rules(
            Filters=[
                {
                    'Name': 'group-id',
                    'Values': group_ids
                },
            ],
        )['SecurityGroupRules']


if __name__ == '__main__':
    # describe security group rules
    ec2sg = EC2SecurityGroup()
    ec2sg.get_security_resources()
    group_id_list = [x for x in ec2sg.sg_with_ec2 if ec2sg.sg_with_ec2[x]]          ## 사용되는 SG
    ec2sg.describe_security_group_rules(group_id_list)
    #for x in ec2sg.sgr_list:
    #    print("######### sgr: " + json.dumps(x, indent=4))
    for x in ec2sg.security_groups:
        print("######### sg: " + json.dumps(x, indent=4))
    print("######### sg with ec2: " + json.dumps(ec2sg.sg_with_ec2, indent=4))
    for x in ec2sg.all_ec2:
        print(ec2sg.all_ec2[x])

