from ast import Index
import pandas as pd
from pprint import pprint


if __name__=="__main__":
    input_ranking_panda=pd.read_csv('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_4_ranking_output/ranking_panda.csv',sep='@')
    input_ranking_panda['url']=''


    url_input_file_address='/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_3_cfmid_identification/url_records.txt'
    print(input_ranking_panda)
    index_to_url_map=dict()

    url_input_file=open(url_input_file_address,'r')
    temp_index=''
    temp_url=''
    #just_found=True
    for line in url_input_file:

        if 'index' in line:
            temp_index=int(line.split(' ')[1].strip())
            #just_found=True
        elif 'identify_url' in line:
            temp_url=line.split(' ')[1].strip()
            index_to_url_map[temp_index]=temp_url
        else:
            continue

    pprint(index_to_url_map)

    cfmid_input_panda=pd.read_pickle('~/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_2_combined_pandas/cfmid_input.bin')

    #for every index in our dict
    # get the unknown from the cfmid input panda
    # use that unknown as the row indicate for the final output panda
    for temp_value in index_to_url_map.keys():
        unknown_in_cfmid_input_panda=cfmid_input_panda.at[temp_value,'identifier']

        correct_index_in_final_input_panda=input_ranking_panda.loc[
            input_ranking_panda.identifier==unknown_in_cfmid_input_panda
        ].index.values[0]

        print(correct_index_in_final_input_panda)

        input_ranking_panda.at[correct_index_in_final_input_panda,'url']=index_to_url_map[temp_value]

    print(input_ranking_panda)
    input_ranking_panda.to_csv('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_5_ranking_output_with_url/cfmid_ranking_with_url.csv',sep='@',index=False)
