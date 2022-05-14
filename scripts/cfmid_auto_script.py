import numpy as np
import pandas as pd
import requests
import time
from pprint import pprint
from bs4 import BeautifulSoup
import os


def generate_identify_post_request(series):

    print(series)
    #databases set to all by just listing them
    ms1_tolerance=series['ms1_tolerance']
    ms2_tolerance=series['ms2_tolerance']
    ms1_tolerance_unit=series['ms1_tolerance_unit']
    ms2_tolerance_unit=series['ms2_tolerance_unit']
    precursor_mass=series['precursor_mass']
    adduct=series['adduct']
    ion_mode=series['ion_mode']
    energy0=series['energy0']
    energy1=series['energy1']
    energy2=series['energy2']
    scoring_function=series['scoring_function']
    
    payload={
        "utf8": '✓',
        "authenticity_token": 'bTHflmDFPhJWri+q0Z3mD86/rsRTbPjGHQ/9C6523VgvRYd4hwqJTSwU9hybFOWiL1SlTK1FJgbv7duGYykB2A==',
        "submit_or_find_or_nl": 'find',
        "identify_query[candidates]": '',
        "identify_query[candidates_file]": '(binary)',
        "identify_query[spectra_type]": 'ESI',
        "identify_query[ion_mode]": 'positive', #seems to always be positive?
        "identify_query[predicted_database][]": ['ChEBI','DrugBank','DSSTox','ECMDB','FooDB','HMDB','KEGG','LIPID MAPS','MassBankJP/MassBankEU','MoNA','NP-MRD','STOFF-IDENT','YMDB'],
        "identify_query[experimental_database][]": ['HMDB','MassBankJP/MassBankEU','MoNA','TMIC'],
        # "identify_query[predicted_database][]": 'DrugBank',
        # "identify_query[predicted_database][]": 'DSSTox',
        # "identify_query[predicted_database][]": 'ECMDB',
        # "identify_query[predicted_database][]": 'FooDB',
        # "identify_query[predicted_database][]": 'HMDB',
        # "identify_query[predicted_database][]": 'KEGG',
        # "identify_query[predicted_database][]": 'LIPID MAPS',
        # "identify_query[predicted_database][]": 'MassBankJP/MassBankEU',
        # "identify_query[predicted_database][]": 'MoNA',
        # "identify_query[predicted_database][]": 'NP-MRD',
        # "identify_query[predicted_database][]": 'STOFF-IDENT',
        # "identify_query[predicted_database][]": 'YMDB',
        # "identify_query[experimental_database][]": 'HMDB',
        # "identify_query[experimental_database][]": 'MassBankJP/MassBankEU',
        # "identify_query[experimental_database][]": 'MoNA',
        # "identify_query[experimental_database][]": 'TMIC',
        "adduct_search_spectra_type": 'ESI',
        "adduct_search_ion_mode": ion_mode,
        "identify_query[adduct_type]": adduct,
        "identify_query[parent_ion_mass]": precursor_mass,
        "parent_ion_mass_type": 'Original',
        "candidate_mass_tol": ms1_tolerance,
        "candidate_mass_tol_units": ms1_tolerance_unit,
        "identify_query[candidate_limit]": 100,
        "text_or_file": 'text',
        "low_spectra": energy0,
        "medium_spectra": energy1,
        "high_spectra": energy2,
        "identify_query[input_file]": '(binary)',
        "identify_query[scoring_function]": scoring_function,
        "identify_query[num_results]": 25,
        "mass_tol": ms2_tolerance,
        "mass_tol_units": ms2_tolerance_unit,
        "identify_query[threshold]": 0.001,
        "commit": 'Submit'
    }   

    try:
        payload['low_spectra']=payload['low_spectra'].replace('\\n','\n')
    except AttributeError:
        pass
    try:
        payload['medium_spectra']=payload['medium_spectra'].replace('\\n','\n')
    except AttributeError:
        pass
    try:
        payload['high_spectra']=payload['high_spectra'].replace('\\n','\n')
    except AttributeError:
        pass
    pprint(payload)
    print(series['energy0'])
    print(energy0)
    identify_request=requests.post(
       'https://cfmid.wishartlab.com/identify/new',
        data=payload
    )
    print(identify_request.url)
    #hold=input('hold')
    return identify_request.url#,request.status_code

def convert_smiles_to_inchikey(smiles):
    return os.popen(f'obabel -:"{smiles}" -oinchikey').read().strip()

if __name__ == "__main__":

    cfmid_input_address='/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_2_combined_pandas/cfmid_input.bin'
    input_panda=pd.read_pickle(cfmid_input_address)
    #swap nan with None
    input_panda=input_panda.replace({np.nan:None})


    for index,series in input_panda.iterrows():
        if index<=5 or index >20:
            continue

        # print(series)
        # print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        
        prediction_dict={
            'rank':[],
            'inchi':[],
            'smiles':[],
            'direct_parent':[],
            'alternative_parents':[]
        }

        #cfmid doesnt handle this adduct
        if series['adduct']=='[M+FA-H]-':
            prediction_dict['rank'].append('adduct_[M+FA-H]-')
            prediction_dict['inchi'].append('adduct_[M+FA-H]-')
            prediction_dict['smiles'].append('adduct_[M+FA-H]-')
            prediction_dict['direct_parent'].append('adduct_[M+FA-H]-')
            prediction_dict['alternative_parents'].append('adduct_[M+FA-H]-')
            prediction_panda=pd.DataFrame.from_dict(prediction_dict)
            prediction_panda['inchikey']=series['inchikey']
            prediction_panda.to_pickle('../step_3_cfmid_identification/cfmid_identification_'+series['identifier']+'_result_adduct.bin')             
            continue


        #result_url,result_code=generate_identify_post_request(series)
        identify_url=generate_identify_post_request(series)

        time.sleep(60)

        get_hashed_result=requests.get(
            identify_url[:-7]
        )

        time.sleep(60)

        get_code=get_hashed_result.status_code

        print('index '+str(index))
        print('identify_url '+identify_url)
        print('get_code '+get_code)

        #if we get the "Something went wrong" webpage
        if get_code==500:
            prediction_dict['rank'].append('result_code_500')
            prediction_dict['inchi'].append('result_code_500')
            prediction_dict['smiles'].append('result_code_500')
            prediction_dict['direct_parent'].append('result_code_500')
            prediction_dict['alternative_parents'].append('result_code_500')
            prediction_panda=pd.DataFrame.from_dict(prediction_dict)
            prediction_panda['inchikey']=series['inchikey']
            prediction_panda.to_pickle('../step_3_cfmid_identification/cfmid_identification_'+series['identifier']+'_result_500.bin')   

        #if the query succeeds on a web-design level
        elif get_code==200:

            to_be_parsed=get_hashed_result.text
            soup=BeautifulSoup(to_be_parsed,'html.parser')
            result_table_rows=soup.find_all("table",class_="identify-display-table")[0].find_all("tr")

            #if the query finds nothing - "failed" on the status bar at the top (only 1 row in result table)
            if len(result_table_rows)==1:
                prediction_dict['rank'].append('no_ids_MADE')
                prediction_dict['inchi'].append('no_ids_MADE')
                prediction_dict['smiles'].append('no_ids_MADE')
                prediction_dict['direct_parent'].append('no_ids_MADE')
                prediction_dict['alternative_parents'].append('no_ids_MADE')
                prediction_panda=pd.DataFrame.from_dict(prediction_dict)
                prediction_panda['inchikey']=series['inchikey']
                prediction_panda.to_pickle('../step_3_cfmid_identification/cfmid_identification_'+series['identifier']+'_result_noids.bin')

            #if we have a "traditional" "successful" query
            elif len(result_table_rows)>1:

                #go through each of the rows in the result table
                for i,row in enumerate(result_table_rows):
                    if i==0:
                        continue
                    for j, column in enumerate(row.children):
                        if j==0:
                            prediction_dict['rank'].append(str(list(column.children)[0]))
                        elif j==5:
                            prediction_dict['inchi'].append(str(list(column.children)[0]))
                            prediction_dict['smiles'].append(str(list(column.children)[2]))
                        elif j==6:
                            prediction_dict['direct_parent'].append(str(column.find("div",class_="classification").string))
                            other_classes=[
                                str(element.string) for element in column.find_all("li")
                            ]
                            prediction_dict['alternative_parents'].append(other_classes)
                



                # pprint(prediction_dict)

                prediction_panda=pd.DataFrame.from_dict(prediction_dict)
                # print(prediction_panda)
                # print(series['identifier'])

                prediction_panda['inchikey']=prediction_panda['smiles'].apply(convert_smiles_to_inchikey)
                prediction_panda.to_pickle('../step_3_cfmid_identification/cfmid_identification_'+series['identifier']+'_result_success.bin')