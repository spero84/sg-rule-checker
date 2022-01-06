import copy
import time
from datetime import datetime
import json
import boto3


# pre-define set
# workgroup = 'fl084d52ab73453ec72daily2021120820211208WorkGroup'
# query_name = 'get-vpc-flowlogs'
# vpc = 'vpc-0a9f9560a12b70705 / egress-test-nfw-01'
# vpc = 'vpc-05d9ecf29ce682669 / spoke-a-test-nfw-01'
# region_names = ['ap-northeast-2']

d = datetime.now()

query_string_1= "Select Distinct account_id, interface_id, srcaddr, srcport, dstaddr, dstport, protocol, action from "
query_string_2 = " where action = \'ACCEPT\' and year='" + str(d.year) + "' and month='" + str(d.month)+ "' and day='" + str(d.day-1) + "' Order by srcaddr, srcport, dstaddr, dstport, protocol"

def get_datetime():
    return '_' + str(d.year) + str(d.month) + str(d.day-1)

class SecurityInfo:

    vpc_id = ''
    flow_logs_id = ''   # just one flow logs available in this case
    athena_data_source_info = ''
    athena_database = ''
    athena_workgroup = ''
    athena_query_name = 'get-5-tuple-'+vpc_id + '-' + flow_logs_id + '-' + get_datetime()

    query_execution_ids = []
    query_result_list = []

    def __init__(self, vpc_id, flow_logs_id, athena_data_source_info,
                 athena_database, athena_workgroup, athena_table):
        self.vpc_id = vpc_id
        self.flow_logs_id = flow_logs_id
        self.athena_data_source_info = athena_data_source_info
        self.athena_database = athena_database
        self.athena_workgroup = athena_workgroup
        self.athena_table = athena_table
        self.athena_query_name = 'get-5-tuple-'+vpc_id + '-' + flow_logs_id + '-' + get_datetime()


class Athena:

    query_execution_results = []
    athena_client = boto3.client('athena')

    def _create_named_query(self, security_info):
        response = self.athena_client.create_named_query(
            Name=security_info.athena_query_name,
            Description='query for vpc flowlogs to get 5 tuples',
            Database=security_info.athena_database,
            QueryString=query_string_1 + security_info.athena_table + query_string_2,
            WorkGroup=security_info.athena_workgroup
        )
        return response

    def _list_named_query(self, workgroup):
        return self.athena_client.list_named_queries(
            WorkGroup=workgroup
        )

    def _batch_get_named_query(self, named_query_response, security_info):
        named_query = self.athena_client.batch_get_named_query(
            NamedQueryIds=named_query_response['NamedQueryIds']
        )
        query_info_list = []
        for query in named_query['NamedQueries']:
            if query['Name'] == security_info.athena_query_name:
                query_info_list.append(copy.deepcopy(query))
                # print(query_info['QueryString'])
        return query_info_list

    def _start_query_execution(self, query_info_list, security_info):
        # print("###" + query_info['QueryString'])
        res = []
        for query_info in query_info_list:
            res.append(self.athena_client.start_query_execution(
                QueryString=query_info['QueryString'],
                QueryExecutionContext={
                    'Database': security_info.athena_database,
                    'Catalog': security_info.athena_data_source_info
                },
                WorkGroup=security_info.athena_workgroup
            )
            )
            print('##### start query : '+json.dumps(res[len(res)-1]['QueryExecutionId'], indent=4))

        return res

    def stop_query_execution(self, query_execution_id):
        self.athena_client.stop_query_execution(
            QueryExecutionId=query_execution_id
        )
    def _get_query_result1(self, query_execution_id):
        return self.athena_client.get_query_results(
            QueryExecutionId=query_execution_id
        )
    def _get_query_result2(self, query_execution_id, next_token):
        return self.athena_client.get_query_results(
            QueryExecutionId=query_execution_id,
            NextToken=next_token
        )

    # main function of this py file
    def get_athena_result(self, security_info):
        # to get named query id
        named_res = self._list_named_query(security_info.athena_workgroup)
        # to get name query string
        query_info_list = self._batch_get_named_query(named_res, security_info)
        print(json.dumps(query_info_list, indent=4))
        if query_info_list:
            print('********** it is not empty!!!!')
            pass
        else:
            named_query_id = self._create_named_query(security_info)
            named_query_ids = {"NamedQueryIds": [named_query_id['NamedQueryId']]}
            query_info_list = self._batch_get_named_query(named_query_ids, security_info)   # NamedQueryIds
            print(json.dumps(query_info_list, indent=4))

            # execute query
        query_execution_results = self._start_query_execution(query_info_list, security_info)
        # stop_query_execution(query_execution_id)
        # get query result
        time.sleep(20)
        # check result
        for query_exe in query_execution_results:
            result_list = self._get_query_result1(query_exe['QueryExecutionId'])
            query_result = result_list['ResultSet']['Rows']
            # print("######## query_result: " + json.dumps(query_result, indent=4))
            col_names = query_result[0]['Data']

            try:
                while result_list['NextToken']:
                    # print('###### Have Next Token!!!!!!!!!!')
                    # print("######### result_list['NextToken'] : " + result_list['NextToken'] )
                    result_list = self._get_query_result2(query_exe['QueryExecutionId'], result_list['NextToken'])
                    query_result = query_result + copy.deepcopy(result_list['ResultSet']['Rows'])
                    # print('####### result_list :' + json.dumps(query_result, indent=4))
                    print("######## len: " + len(query_result).__str__())
                # TODO 실제 결과 가져오는 부분!! 수정 필요
                # print('####### len : ' + str(len(query_result)))
                # print(json.dumps(query_result, indent=4))
            except Exception as e:
                print(e)

            # print("####### col_name : " + json.dumps(col_names))
            for result in query_result[1:]:
                res = {}
                for x in range(0, 8):
                    # print("##### varchar : " + col_names[x]["VarCharValue"])
                    # print("##### varchar : " + result['Data'][x]["VarCharValue"])
                    res[col_names[x]["VarCharValue"]] = result['Data'][x]["VarCharValue"]
                # print("####### athena query result: " + json.dumps(res, indent=4))
                security_info.query_result_list.append(copy.deepcopy(res))


'''
if __name__ == '__main__':
    # to get named query id
    response = _list_named_query()
    # to get name query string
    _get_named_query(response)
    # print(query_info)
    # execute query
    query_execution_id = _start_query_execution()
    #_stop_query_execution(query_execution_id)
    # get query result
    time.sleep(5)
    result = get_query_result(query_execution_id)
    print(json.dumps(result, indent=4))
'''