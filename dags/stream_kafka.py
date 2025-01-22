from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'rakesh',
    'start_date': datetime(2025, 1, 22, 3, 0)
}

def get_data_from_api():
    import json
    import requests

    res = requests.get('https://randomuser.me/api')
    res = res.json()
    res = res['results'][0]
    # print(json.dumps(res, indent = 3))
    return res
    
def data_formatting(res):
    data = {}
    location  = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {str(location['street']['name'])} " \
                        f"{str(location['city'])}, {str(location['state'])}, {str(location['country'])}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['cell']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    import json
    res = get_data_from_api()
    res = data_formatting(res)
    print(json.dumps(res, indent = 3))
    

stream_data()


# with DAG('kafka_automation',
#          default_args = default_args,
#          catchup = False)  as dag:
#     kafka_streaming_task = PythonOperator(
#         task_id = 'data_stream_from_api',
#         python_callable = stream_data
#     )