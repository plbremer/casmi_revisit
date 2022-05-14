import numpy as np
import pandas as pd
from pprint import pprint



if __name__=="__main__":

    msp_address_list=[
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/input/casmi_neg35.txt',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/input/casmi_neg45.txt',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/input/casmi_neg65.txt',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/input/casmi_pos35.txt',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/input/casmi_pos45.txt',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/input/casmi_pos65.txt',
    ]

    panda_output_list=[
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_neg35.bin',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_neg45.bin',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_neg65.bin',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_pos35.bin',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_pos45.bin',
        '/home/rictuar/coding_projects/fiehn_work/assistance_arpana/cfmid_identification/step_1_parsed_msps/casmi_pos65.bin',        
    ]



    for i in range(len(panda_output_list)):
        output_dict={
            'name':[],
            'inchikey':[],
            'precursor_mass':[],
            'adduct':[],
            'energy':[],
            'ion_mode':[],
            'ms1_intensity':[],
            'spectrum':[]
        }

        peaks_remaining=0
        temp_spectrum=''
        temp_file=open(msp_address_list[i],'r')

        for line in temp_file:
            line=line.strip()
            parts=line.split(': ')
            if parts[0]=='Name':
                output_dict['name'].append(parts[1])
            elif parts[0]=='PrecursorMZ':
                output_dict['precursor_mass'].append(parts[1])
            elif parts[0]=='InChIKey':
                output_dict['inchikey'].append(parts[1])
            elif parts[0]=='Precursor_type':
                output_dict['adduct'].append(parts[1])
            elif parts[0]=='Collision_enerty':
                output_dict['energy'].append(parts[1])
            elif parts[0]=='Ion_mode':
                output_dict['ion_mode'].append(parts[1])
            elif parts[0]=='Comment':
                ms1_intensity=parts[1].split(' intensity ')[1]
                output_dict['ms1_intensity'].append(ms1_intensity)
            elif parts[0]=='Num peaks':
                peaks_remaining=int(parts[1])
            elif peaks_remaining>0:
                temp_spectrum=temp_spectrum+line+'\n'
                peaks_remaining-=1
            elif parts[0]=='':
                output_dict['spectrum'].append(temp_spectrum)
                temp_spectrum=''
            
            # pprint(output_dict)
            # hold=input('hold')
        temp_file.close()
        output_panda=pd.DataFrame.from_dict(output_dict)
        output_panda.ms1_intensity=output_panda.ms1_intensity.astype(int)
        #choose the row with the greateest ms1 intensity
        output_panda=output_panda.iloc[output_panda.groupby(by=['name','adduct']).ms1_intensity.idxmax()]
        output_panda=output_panda.reset_index(drop=True)
        output_panda.to_pickle(panda_output_list[i])
