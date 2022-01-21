import ipaddress
import json

from get_athena_result import Athena
from get_athena_result import SecurityInfo
from get_sg_rule import EC2SecurityGroup
from put_dynamodb import DynamoDB


protocol_list = {'1':'icmp', '2': 'igmp', '6': 'tcp', '17': 'udp', '47': 'gre'}

if __name__ == '__main__':
    security_info_list = []
    # spoke-a-test-nfw-01
    security_info_list.append(SecurityInfo(
        vpc_id='vpc-06545e3c212af746f',
        flow_logs_id='fl-021e77eb542d6567f',
        athena_data_source_info='AwsDataCatalog',
        athena_database = 'vpcflowlogsathenadatabasefl021e77eb542d6567f',
        athena_workgroup = 'fl021e77eb542d6567fdaily2021122920211229WorkGroup',
        athena_table = 'fl021e77eb542d6567fdaily2021122920211229'))

    security_info_list.append(SecurityInfo(
            vpc_id='vpc-05d9ecf29ce682669 ',
            flow_logs_id='fl-022240b45616482b1',
            athena_data_source_info='AwsDataCatalog',
            athena_database = 'vpcflowlogsathenadatabasefl022240b45616482b1',
            athena_workgroup = 'fl022240b45616482b1daily2021122920211229WorkGroup',
            athena_table = 'fl022240b45616482b1daily2021122920211229') )

    security_info_list.append(SecurityInfo(
        vpc_id='vpc-068f9147fa6018e6d',
        flow_logs_id='fl-0287b4386ad838dca',
        athena_data_source_info='AwsDataCatalog',
        athena_database = 'vpcflowlogsathenadatabasefl0287b4386ad838dca',
        athena_workgroup = 'fl0287b4386ad838dcadaily2021122920211229WorkGroup',
        athena_table = 'fl0287b4386ad838dcadaily2021122920211229') )

    security_info_list.append(SecurityInfo(
        vpc_id='vpc-0dc0216651ed344b5',
        flow_logs_id='fl-03653ef6c3362f341',
        athena_data_source_info='AwsDataCatalog',
        athena_database = 'vpcflowlogsathenadatabasefl03653ef6c3362f341',
        athena_workgroup = 'fl03653ef6c3362f341daily2021122920211229WorkGroup',
        athena_table = 'fl03653ef6c3362f341daily2021122920211229') )

    security_info_list.append(SecurityInfo(
        vpc_id='vpc-0c70b986a264a86b1',
        flow_logs_id='fl-03d0cc9fe793fdeaf',
        athena_data_source_info='AwsDataCatalog',
        athena_database = 'vpcflowlogsathenadatabasefl03d0cc9fe793fdeaf',
        athena_workgroup = 'fl03d0cc9fe793fdeafdaily2021122920211229WorkGroup',
        athena_table = 'fl03d0cc9fe793fdeafdaily2021122920211229') )

    security_info_list.append(SecurityInfo(
        vpc_id='vpc-0556def2dfbb830f2',
        flow_logs_id='fl-04d04cae87355bdd6',
        athena_data_source_info='AwsDataCatalog',
        athena_database = 'vpcflowlogsathenadatabasefl04d04cae87355bdd6',
        athena_workgroup = 'fl04d04cae87355bdd6daily2021122920211229WorkGroup',
        athena_table = 'fl04d04cae87355bdd6daily2021122920211229') )

    security_info_list.append(SecurityInfo(
        vpc_id='vpc-0452b7a82e11954dc',
        flow_logs_id='fl-09f6a1ac2523ef42f',
        athena_data_source_info='AwsDataCatalog',
        athena_database = 'vpcflowlogsathenadatabasefl09f6a1ac2523ef42f',
        athena_workgroup = 'fl09f6a1ac2523ef42fdaily2021122920211229WorkGroup',
        athena_table = 'fl09f6a1ac2523ef42fdaily2021122920211229') )

    security_info_list.append(SecurityInfo(
        vpc_id='vpc-0a9f9560a12b70705',
        flow_logs_id='fl-0aecca929e9eb0f4a',
        athena_data_source_info='AwsDataCatalog',
        athena_database = 'vpcflowlogsathenadatabasefl0aecca929e9eb0f4a',
        athena_workgroup = 'fl0aecca929e9eb0f4adaily2021122920211229WorkGroup',
        athena_table = 'fl0aecca929e9eb0f4adaily2021122920211229') )

    security_info_list.append(SecurityInfo(
        vpc_id='vpc-0da9dd6c2a18796f7',
        flow_logs_id='fl-0dac6993748e86a30',
        athena_data_source_info='AwsDataCatalog',
        athena_database = 'vpcflowlogsathenadatabasefl0dac6993748e86a30',
        athena_workgroup = 'fl0dac6993748e86a30daily2021122920211229WorkGroup',
        athena_table = 'fl0dac6993748e86a30daily2021122920211229') )

    vpc_id_list = []
    for sil in security_info_list:
        vpc_id_list.append(sil.vpc_id)

    print("######### : " + json.dumps(vpc_id_list))

    # Athena query
    athena = Athena()
    for si in security_info_list:
        athena.get_athena_result(security_info=si)
        '''
        for res in si.query_execution_results:
            print("####### si result: " + json.dumps(res, indent=4))
        '''

    ec2sg = EC2SecurityGroup(vpc_id_list)
    ec2sg.get_security_resources()
    group_id_list = [x for x in ec2sg.sg_with_ec2 if ec2sg.sg_with_ec2[x]]      # 실제 사용되는 sgr list
    print("#######3 group_id_list : " + str(group_id_list))
    ec2sg.describe_security_group_rules(group_id_list)

    # create dynamodb tables
    dynamodb = DynamoDB()
    dynamodb.create_unused_rule_table()
    dynamodb.create_not_assigned_table()

    not_assigned_sg_put_list = []
    assigned_sg_list = {}

    # erase and put to dynamodb not assigned security groups
    for key, val in list(reversed(ec2sg.sg_with_ec2.items())):  # reverse dict for loop logic
        if not val:  # not assigned security group
            item = {"SGId": {'S': key}}
            dynamodb.put_item(dynamodb.not_assigned_sg_table_name + dynamodb.get_datetime(), item)
            not_assigned_sg_put_list.append(key)
            del ec2sg.sg_with_ec2[key]

    print("####### size of sgr list :" + str(len(ec2sg.sgr_list)))
    # check logic
    for sil in security_info_list:  # multi vpc flow logs
        # print("#######33 sil.query_execution_results size : " + str(len(sil.query_result_list)))
        for vfl in sil.query_result_list:  # vpc flow logs
            # print("#####3 ec2sg.sgr_list size : " + str(len(ec2sg.sgr_list)))
            if not ec2sg.sgr_list:
                print("###### break loop")
                break

            for sgr in reversed(ec2sg.sgr_list):   # sgr list, for remove rule, reverse the for loop
                chk = False
                for sg in ec2sg.sg_with_ec2[sgr['GroupId']]:
                    for sg_eni in sg['interface_id']:       # ec2 has multi eni
                        # print("##### sgr" + json.dumps(sgr, indent=4))
                        # print("##### sg" + json.dumps(sg, indent=4))
                        # print("#### vfl " + json.dumps(vfl, indent=4))
                        # print("########## sg_eni == vfl['interface_id'] : " + sg_eni + ' , ' + vfl['interface_id'])
                        # print("########## sg_eni == vfl['interface_id'] : " + str( sg_eni==vfl['interface_id']) )
                        # print("#######3  protocol_list[vfl['protocol']] == sgr['IpProtocol'] : " + str(protocol_list[vfl['protocol']]==sgr['IpProtocol']))
                        # print("#######3  protocol_list[vfl['protocol']] == sgr['IpProtocol'] : " + str(type(protocol_list[vfl['protocol']])) + ' , ' +  str(type(sgr['IpProtocol'])))

                        if sg_eni == vfl['interface_id'] and (protocol_list[vfl['protocol']] == sgr['IpProtocol'] or sgr['IpProtocol']=='-1'):
                            # print("######## if and protocol are same !!!!!!!!!!!!")
                            if sgr['FromPort'] == -1 and sgr['CidrIpv4'] == '-1':        # any ip / port
                                chk = True
                            elif sgr['CidrIpv4'] == '-1' and \
                                    (vfl['srcport'] == sgr['FromPort'] or vfl['srcport'] == sgr['FromPort']):  # any ip
                                chk = True
                            elif sgr['FromPort'] == -1 and \
                                    (ipaddress.ip_address(vfl['srcaddr']) in ipaddress.ip_network(sgr['CidrIpv4']) or
                                     ipaddress.ip_address(vfl['dstaddr']) in ipaddress.ip_network(sgr['CidrIpv4'])):
                                # any port
                                chk = True
                            elif (ipaddress.ip_address(vfl['srcaddr']) in ipaddress.ip_network(sgr['CidrIpv4']) and
                                  vfl['srcport'] == sgr['FromPort']) or \
                                    (ipaddress.ip_address(vfl['dstaddr']) in ipaddress.ip_network(sgr['CidrIpv4']) and
                                     vfl['srcport'] == sgr['FromPort']):
                                chk = True
                if chk:
                    print("####### remove :" + json.dumps(sgr, indent=4))
                    ec2sg.sgr_list.remove(sgr)

    print("####### size of sgr list :" + str(len(ec2sg.sgr_list)))

    for sgr in ec2sg.sgr_list:
        dstIp = ''
        try:
            dstIp = sgr['CidrIpv4']
        except Exception as e:
            print(e)
            dstIp = sgr['ReferencedGroupInfo']['GroupId']

        item = {'SGRuleId': {'S': sgr['SecurityGroupRuleId']},
                'SGId': {'S': sgr['GroupId']},
                'protocol': {'S': sgr['IpProtocol']},
                'dstIp': {'S': dstIp},
                'dstPort': {'N': str(sgr['FromPort'])},
                'isEgress': {'BOOL': sgr['IsEgress']},
                'ec2': {'SS': [sg['ec2'] for sg in ec2sg.sg_with_ec2[sgr['GroupId']]] },
                'eni': {'SS': [eni for sg in ec2sg.sg_with_ec2[sgr['GroupId']] for eni in sg['interface_id']]},
                'accountId': {'S': sgr['GroupOwnerId']}}
        # print("###### table name : " + dynamodb.unused_rule_table_name + dynamodb.get_datetime())
        print("###### item : " + json.dumps(item, indent=4))
        dynamodb.put_item(dynamodb.unused_rule_table_name + dynamodb.get_datetime(), item)






