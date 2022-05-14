from multiprocessing.sharedctypes import Value
import pandas as pd
import os

#the input to this, for the answer key
#is literally a one column panda where the column name is identifier and the rows are all inchikeys


if __name__ =="__main__":


    answer_key_input_address='/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/input/std_list_corrected.csv'
    answer_key_panda=pd.read_csv(answer_key_input_address)

    cfmid_base_output_address='/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_3_cfmid_identification/'
    output_file_list=os.listdir(cfmid_base_output_address)
    print(output_file_list)

    output_dict={
        'identifier':[],
        'inchikey':[],
        'top_rank_inchi':[],
        'top_rank_smiles':[],
        'top_rank_class':[],
        'rank_of_correct_identity':[],
        'class_of_correct_identity':[]
    }

    for index,series in answer_key_panda.iterrows():


        correct_file_list=[temp_file for temp_file in output_file_list if (series['Name'] in temp_file)]

        print(series)
        print(correct_file_list)

        if len(correct_file_list) > 1:
            print('we found redudnant inchikeys')
            hold=input('hold')
        elif len(correct_file_list)==0:
            output_dict['identifier'].append(series['Name'])
            output_dict['inchikey'].append(series['InChIKey'])
            output_dict['top_rank_inchi'].append('not_in_parker_pipeline_input')
            output_dict['top_rank_smiles'].append('not_in_parker_pipeline_input')
            output_dict['top_rank_class'].append('not_in_parker_pipeline_input')
            output_dict['rank_of_correct_identity'].append('not_in_parker_pipeline_input')
            output_dict['class_of_correct_identity'].append('not_in_parker_pipeline_input')  
            continue          

        correct_file_name=correct_file_list[0]

        if 'adduct' in correct_file_name:
            output_dict['identifier'].append(series['Name'])
            output_dict['inchikey'].append(series['InChIKey'])
            output_dict['top_rank_inchi'].append('adduct_[M+FA-H]-')
            output_dict['top_rank_smiles'].append('adduct_[M+FA-H]-')
            output_dict['top_rank_class'].append('adduct_[M+FA-H]-')
            output_dict['rank_of_correct_identity'].append('adduct_[M+FA-H]-')
            output_dict['class_of_correct_identity'].append('adduct_[M+FA-H]-')
        elif '500' in correct_file_name:
            output_dict['identifier'].append(series['Name'])
            output_dict['inchikey'].append(series['InChIKey'])
            output_dict['top_rank_inchi'].append('500_error')
            output_dict['top_rank_smiles'].append('500_error')
            output_dict['top_rank_class'].append('500_error')
            output_dict['rank_of_correct_identity'].append('500_error')
            output_dict['class_of_correct_identity'].append('500_error')
        elif 'noids' in correct_file_name:
            output_dict['identifier'].append(series['Name'])
            output_dict['inchikey'].append(series['InChIKey'])
            output_dict['top_rank_inchi'].append('no_id')
            output_dict['top_rank_smiles'].append('no_id')
            output_dict['top_rank_class'].append('no_id')
            output_dict['rank_of_correct_identity'].append('no_id')
            output_dict['class_of_correct_identity'].append('no_id')
        elif 'success' in correct_file_name:
            temp_panda=pd.read_pickle(cfmid_base_output_address+correct_file_name)
            temp_panda['inchikey_first_block']=temp_panda.inchikey.str.split('-',expand=True)[0]
            answer_first_block=series['InChIKey'].split('-')[0]
            try:
                best_ranking_of_correct=temp_panda['inchikey_first_block'].to_list().index(answer_first_block)
                output_dict['identifier'].append(series['Name'])
                output_dict['inchikey'].append(series['InChIKey'])
                output_dict['top_rank_inchi'].append(temp_panda.at[0,'inchi'])
                output_dict['top_rank_smiles'].append(temp_panda.at[0,'smiles'])
                output_dict['top_rank_class'].append(temp_panda.at[0,'direct_parent'])
                output_dict['rank_of_correct_identity'].append(best_ranking_of_correct)
                output_dict['class_of_correct_identity'].append(temp_panda.at[best_ranking_of_correct,'direct_parent'])
            except ValueError:
                output_dict['identifier'].append(series['Name'])
                output_dict['inchikey'].append(series['InChIKey'])
                output_dict['top_rank_inchi'].append(temp_panda.at[0,'inchi'])
                output_dict['top_rank_smiles'].append(temp_panda.at[0,'smiles'])
                output_dict['top_rank_class'].append(temp_panda.at[0,'direct_parent'])
                output_dict['rank_of_correct_identity'].append('not_found')
                output_dict['class_of_correct_identity'].append('not_found')

        
    output_panda=pd.DataFrame.from_dict(output_dict)
    output_panda.to_csv('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_4_ranking_output/ranking_panda.csv',sep='@',index=False)