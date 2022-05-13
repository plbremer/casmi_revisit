import numpy as np
import pandas as pd
from pprint import pprint



if __name__=="__main__":

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

    msp_address='/home/rictuar/Downloads/casmi_neg45.txt'

    peaks_remaining=0
    temp_spectrum=''

    temp_file=open(msp_address,'r')
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
        
        pprint(output_dict)
        hold=input('hold')