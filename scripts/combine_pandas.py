
import pandas as pd



if __name__=="__main__":

    neg_35=pd.read_pickle('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_neg35.bin')
    neg_45=pd.read_pickle('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_neg45.bin')
    neg_65=pd.read_pickle('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_neg65.bin')

    neg_35=neg_35.set_index('name',drop=True)
    neg_45=neg_45.set_index('name',drop=True)
    neg_65=neg_65.set_index('name',drop=True)

    neg_35.rename({'spectrum':'energy0'},axis='columns',inplace=True)
    neg_45.rename({'spectrum':'energy1'},axis='columns',inplace=True)
    neg_65.rename({'spectrum':'energy2'},axis='columns',inplace=True)

    excess_column_list=['inchikey', 'precursor_mass', 'adduct', 'energy', 'ion_mode', 'ms1_intensity']
    neg_45.drop(excess_column_list,axis='columns',inplace=True)
    neg_65.drop(excess_column_list,axis='columns',inplace=True)

    combined_neg=neg_35[['inchikey','precursor_mass','adduct','energy0']].copy()

    combined_neg=combined_neg.join(
        other=neg_45,
        how='left'
    )

    combined_neg=combined_neg.join(
        other=neg_65,
        how='left'
    )

    combined_neg['ion_mode']='negative'
    combined_neg['ms1_tolerance']=10
    combined_neg['ms2_tolerance']=10
    combined_neg['ms1_tolerance_unit']='ppm'
    combined_neg['ms2_tolerance_unit']='ppm'
    combined_neg['candidate_limit']=100
    combined_neg['scoring_function']='DotProduct'

    #
    # identifier					precursor m/z	adduct	ion_mode	energy0	energy1	energy2		
    print(combined_neg)






    pos_35=pd.read_pickle('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_pos35.bin')
    pos_45=pd.read_pickle('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_pos45.bin')
    pos_65=pd.read_pickle('/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_pos65.bin')

    pos_35=pos_35.set_index('name',drop=True)
    pos_45=pos_45.set_index('name',drop=True)
    pos_65=pos_65.set_index('name',drop=True)

    pos_35.rename({'spectrum':'energy0'},axis='columns',inplace=True)
    pos_45.rename({'spectrum':'energy1'},axis='columns',inplace=True)
    pos_65.rename({'spectrum':'energy2'},axis='columns',inplace=True)

    excess_column_list=['inchikey', 'precursor_mass', 'adduct', 'energy', 'ion_mode', 'ms1_intensity']
    pos_45.drop(excess_column_list,axis='columns',inplace=True)
    pos_65.drop(excess_column_list,axis='columns',inplace=True)

    combined_pos=pos_35[['inchikey','precursor_mass','adduct','energy0']].copy()

    combined_pos=combined_pos.join(
        other=pos_45,
        how='left'
    )

    combined_pos=combined_pos.join(
        other=pos_65,
        how='left'
    )

    combined_pos['ion_mode']='positive'
    combined_pos['ms1_tolerance']=10
    combined_pos['ms2_tolerance']=10
    combined_pos['ms1_tolerance_unit']='ppm'
    combined_pos['ms2_tolerance_unit']='ppm'
    combined_pos['candidate_limit']=100
    combined_pos['scoring_function']='DotProduct'










    combined=pd.concat(objs=[combined_neg,combined_pos])
    combined=combined.reset_index()
    combined.rename({'name':'identifier'},axis='columns',inplace=True)

    print(combined)

    combined.to_pickle(
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_2_combined_pandas/cfmid_input.bin'
    )