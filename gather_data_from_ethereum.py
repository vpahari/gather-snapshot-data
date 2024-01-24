import pickle
import numpy as np
import pandas as pd
from web3 import Web3

import requests
import json
from concurrent.futures import ThreadPoolExecutor
from tqdm.notebook import tqdm
import itertools

from dotenv import dotenv_values
from eth_abi import abi


eth_node = "http://ethereum-archive:8545"
w3 = Web3(Web3.HTTPProvider(eth_node))

config = dotenv_values(".env")
etherscan_apikey = config['ETHERSCAN_API_KEY']

def get_abi_from_etherscan(contract_address):
    api_url = 'https://api.etherscan.io/api?module=contract&action=getabi&address='
    api_url = api_url + contract_address + '&apikey=' + etherscan_apikey
    response = requests.get(api_url)
    abi = dict()
    if response.status_code == 200:
        #print(response.json())
        abi = response.json()['result']
        #print(abi)
        #print(response)
        abi = json.loads(abi)
    return abi


def get_batch_intervals(block_start, block_end, batch_size):
    # Improved version of the line code below
    # pd.interval_range(start=block_number_min, end=block_number_max, freq=batch_size)
    intervals = list()
    block_numbers = list(range(block_start, block_end, batch_size))
    for block_number in block_numbers:
        block_interval_start = block_number
        block_interval_end = min(block_number + batch_size - 1, block_end)
        intervals.append((block_interval_start, block_interval_end))
    return intervals

def get_event_logs(params):
    start_block,end_block,address, topic = params['start_block'], params['end_block'], params['address'], params['topic']
    #print(start_block,end_block)
    start_block_str = f'"fromBlock" : "{start_block}"'
    to_block_str = f'"toBlock" : "{end_block}"'
    address_str = f'"address" : "{address}"'
    topic_str = f'"topics" : ["{topic}"]'
    comma = ','
    data_call_init_str = '[{' + start_block_str + comma + to_block_str + comma + address_str + comma + topic_str + '}], "jsonrpc":"2.0","id":1}'
    data_call = '{"method":"eth_getLogs","params":' + data_call_init_str 
    #print(data_call)
    header = {'Content-Type': 'application/json'}
    r = requests.post(eth_node,data=data_call,headers=header)
    result_curr = json.loads(r.content.decode('utf8').replace("'", '"'))['result']
    return result_curr

def get_event_logs_in_batches(start_block, end_block, address, topic, batch_size = 5000, max_workers=20):
    event_list = list()
    intervals = get_batch_intervals(block_start=start_block, block_end=end_block, batch_size=batch_size)
    params = []
    for interval_start, interval_end in intervals:
        params.append({'start_block':interval_start, 'end_block':interval_end, 'address':address, 'topic':topic})
    #print(params)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        event_list = list(tqdm(pool.map(get_event_logs, params), total=len(intervals), desc=topic))
    event_list = list(itertools.chain(*event_list))
    return event_list
    
def decode_data_from_receipt(data_list, data):
    curr_data = bytes.fromhex(data[2:])
    decodedABI = abi.decode(data_list, curr_data)
    return decodedABI

def get_event_data_from_abi(curr_abi, event_name):
    true_events_types = []
    false_events_types = []
    true_events_name = []
    false_events_name = []
    for k in curr_abi:
        if 'name' not in k:
            continue
        if k['name'] == event_name and k['type'] == 'event':
            for current_input in k['inputs']:
                if current_input['indexed']:
                    true_events_types.append(current_input['type'])
                    true_events_name.append(current_input['name'])
                else:
                    false_events_types.append(current_input['type'])
                    false_events_name.append(current_input['name'])
    return true_events_types, true_events_name, false_events_types, false_events_name

def decode_list(all_events_list, abi_to_use, event_name):
    true_data_list, true_data_name, false_data_list, false_data_name = \
        get_event_data_from_abi(abi_to_use, event_name)
    events_decoded_list = []
    for event in all_events_list:
        dict_name_to_value = {}
        counter = 0
        #print(true_data_list, true_data_name, false_data_list, false_data_name)
        for k in event['topics'][1:]:
            curr_decoded = decode_data_from_receipt([true_data_list[counter]], k)
            dict_name_to_value[true_data_name[counter]] = curr_decoded[0]
            counter += 1
        decoded_false = decode_data_from_receipt(false_data_list,event['data'])
        for counter in range(len(false_data_name)):
            dict_name_to_value[false_data_name[counter]] = decoded_false[counter]
        dict_name_to_value['address'] = event['address']
        dict_name_to_value['blockNumber'] = int(event['blockNumber'], 0)
        dict_name_to_value['transactionHash'] = event['transactionHash']
        dict_name_to_value['transactionIndex'] = event['transactionIndex']
        dict_name_to_value['blockHash'] = event['blockHash']
        dict_name_to_value['logIndex'] = event['logIndex']
        dict_name_to_value['removed'] = event['removed']
        events_decoded_list.append(dict_name_to_value)
    return events_decoded_list

def decode_list_with_params(params):
    curr_events_list = params['events_list']
    curr_abi = params['abi']
    curr_event_name = params['event_name']
    return decode_list(curr_events_list, curr_abi, curr_event_name)

def decode_list_with_tqdm(all_events, abi_to_use, event_name, batch_size = 5000, max_workers=20):
    event_list = list()
    intervals = get_batch_intervals(block_start=0, block_end=len(all_events), batch_size=batch_size)
    params = []
    for interval_start, interval_end in intervals:
        params.append({'events_list':all_events[interval_start : interval_end + 1], 'abi':abi_to_use, 'event_name':event_name})
    #print(params)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        event_list = list(tqdm(pool.map(decode_list_with_params, params), total=len(intervals), desc=event_name))
    event_list = list(itertools.chain(*event_list))
    return event_list

def get_topic_from_topic_name(curr_abi,topic_name):
    str_to_ret = ''
    for k in curr_abi:
        if 'name' not in k:
                continue
        if k['name'] == topic_name and k['type'] == 'event':
            for current_input in k['inputs']:
                print(current_input['type'])
                str_to_ret += current_input['type'] + ','
    event_function_name = topic_name + "(" + str_to_ret[:-1] + ")"
    return w3.keccak(text=event_function_name).hex()

def get_decoded_events_logs(start_block, end_block, address, curr_abi, topic_name, batch_size = 5000, max_workers=20):
    topic_hex = get_topic_from_topic_name(curr_abi,topic_name)
    events_list = get_event_logs_in_batches(start_block=start_block, end_block=end_block,address=address,topic=topic_hex, batch_size=batch_size, max_workers=max_workers)
    decoded_events_list = decode_list_with_tqdm(events_list, curr_abi, topic_name, batch_size=batch_size, max_workers=max_workers)
    return decoded_events_list
    

