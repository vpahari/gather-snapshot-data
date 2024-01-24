import requests
import time
import pandas as pd

def get_proposal_string(ens_address):
    ens_address_str = "\"" + ens_address + "\""
    proposal_string = '''query {
    proposals (
        first: 1000,
        skip: 0,
        where: {
        space_in: [%s],
        state: "closed"
        },
        orderBy: "created",
        orderDirection: desc
    ) {
        id
        title
        choices
        start
        end
        snapshot
        state
        scores
        scores_by_strategy
        scores_total
        scores_updated
        author
    }
    }'''%(ens_address_str)
    return proposal_string

def get_proposals_from_project(ens_address):
        full_proposal_list = []
        skip_num = 0
        n_retry = 0
        while True:
            if n_retry >= 5:
                return None
            curr_proposal_query = get_proposal_string(ens_address)
            curr_response = requests.get(url="https://hub.snapshot.org/graphql", json={"query": curr_proposal_query})
            if curr_response.status_code == 200:
                curr_proposal_list = curr_response.json()['data']['proposals']
                full_proposal_list += curr_proposal_list
                if skip_num == 5000:
                    return full_proposal_list
                if len(curr_proposal_list) == 1000:
                    skip_num += 1000
                    continue
                else:
                    break
            else:
                print(curr_response.status_code)
                print(curr_response.json())
                n_retry += 1
                continue
        return full_proposal_list


def get_votes_str(skip, proposal):
    proposal_str = "\"" + proposal + "\""
    votes_str = '''{
    votes (
        first: 1000
        skip: %s
        where: {
        proposal: %s
        }
    ) {
        id
        voter
        vp
        created
        choice
    }
    }'''%(skip, proposal_str)
    return votes_str


def get_votes_of_single_proposal(curr_proposal_id):
    full_votes_list = []
    skip_num = 0
    n_retry = 0
    while True:
        if n_retry >= 5:
            return None
        curr_vote_query = get_votes_str(skip_num,curr_proposal_id)
        #print(curr_vote_query)
        curr_response = requests.get(url="https://hub.snapshot.org/graphql", json={"query": curr_vote_query})
        #print(curr_response.status_code)
        #print(curr_response.json())
        if curr_response.status_code == 200:
            curr_votes_list = curr_response.json()['data']['votes']
            #print(len(curr_votes_list))
            full_votes_list += curr_votes_list
            if skip_num == 5000:
                return full_votes_list
            if len(curr_votes_list) == 1000:
                skip_num += 1000
                continue
            else:
                break
        else:
            print(curr_response.status_code)
            print(curr_response.json())
            n_retry += 1
            continue
    return full_votes_list
        

def get_votes_df_from_snapshot_ids(shapshot_ids_list):
    df_snapshot = pd.DataFrame()
    for curr_id in shapshot_ids_list:
        curr_votes = get_votes_of_single_proposal(curr_id)
        if curr_votes is None:
            print("ENDED")
            break
        curr_df  =  pd.DataFrame(curr_votes)
        curr_df['proposal_id'] = [curr_id for i in range(len(curr_votes))]
        df_snapshot = pd.concat([df_snapshot, curr_df])
        print(len(df_snapshot.index))
        time.sleep(2)







