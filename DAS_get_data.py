
"""
Census Data DAS Analysis: Part 1
5/21/21

takes raw DP data for ε=4.5 and ε=12.2
parses into all 50 states
groups data frame by block geoid
runs for all states and writes to file


"""

import pandas as pd
import geopandas as gpd
import time
import os

def getStateData(st_fips, csize, fname):
    '''
    
    for a single state and input file, parse the file into chunks 
    and search for entries from the state
    
    return the full dataframe with all entries from the state

    '''
    st_df = pd.DataFrame()
    i = 1
    for chunk in pd.read_csv(fname, chunksize=csize):
        print("chunk number: {0}".format(i))
        data = chunk
        sts = chunk['TABBLKST'].unique()
        print("states: {0}".format(sts))
        
        if st_fips in sts:
            print("appending state data from chunk for {0}".format(st_fips))
            st_data = chunk.loc[chunk['TABBLKST'] == st_fips]
            print(len(st_df))
            if(len(st_df)<1):
                st_df = st_data
            else:
                st_df = st_df.append(st_data)
                print(st_data['TABBLKCOU'].unique())
                print(len(st_data))
        else:
            print("no state data for {0}".format(st_fips))
        i = i+1
    return st_df

def cleanData(df):
    
    '''
    
    take a subset of the census data and clean the column names
    then sort by GEOID to get 1 entry for each populated block
    
    return dataframe grouped by GEOID

    '''
    # clean column names and types
    df['TABBLKST'] = df['TABBLKST'].map(lambda x:str(x).zfill(2))
    df['TABBLKCOU'] = df['TABBLKCOU'].map(lambda x:str(x).zfill(3))
    df['TABTRACTCE'] = df['TABTRACTCE'].map(lambda x:str(x).zfill(6))
    df['TABBLKGRPCE'] = df['TABBLKGRPCE'].map(lambda x:str(x))
    df['TABBLK'] = df['TABBLK'].map(lambda x:str(x).zfill(4))
    
    df['GEOID'] = df['TABBLKST'] + df['TABBLKCOU'] + df['TABTRACTCE'] + df['TABBLK']
    
    # list all the 2more races variables
    cols_2mo = list(range(7, 64))

    # number of entries per GEOID (each block)
    df_sort = pd.DataFrame(df.groupby('GEOID')['GEOID'].count())
    
    # VAP total
    df_sort['totVAPDP'] = df.loc[df['VOTING_AGE'] == 2].groupby('GEOID')['GEOID'].count()
    df_sort['nonVAPDP'] = df.loc[df['VOTING_AGE'] == 1].groupby('GEOID')['GEOID'].count()
    
    # HVAP 
    df_sort['HVAPDP'] = df.loc[(df['CENHISP'] == 2) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    df_sort['NHVAPDP'] = df.loc[(df['CENHISP'] == 1) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    
    # hispanic / nonhispanic total
    df_sort['HtotDP'] =  df.loc[df['CENHISP'] == 2].groupby('GEOID')['GEOID'].count()
    df_sort['NHtotDP'] =  df.loc[df['CENHISP'] == 1].groupby('GEOID')['GEOID'].count()
    
    # each race alone total pop
    df_sort['whi_aloDP']  = df.loc[df['CENRACE'] == 1].groupby('GEOID')['GEOID'].count()
    df_sort['bla_aloDP']  = df.loc[df['CENRACE'] == 2].groupby('GEOID')['GEOID'].count()
    df_sort['nat_aloDP']  = df.loc[df['CENRACE'] == 3].groupby('GEOID')['GEOID'].count()
    df_sort['asi_aloDP']  = df.loc[df['CENRACE'] == 4].groupby('GEOID')['GEOID'].count()
    df_sort['pci_aloDP']  = df.loc[df['CENRACE'] == 5].groupby('GEOID')['GEOID'].count()
    df_sort['sor_aloDP']  = df.loc[df['CENRACE'] == 6].groupby('GEOID')['GEOID'].count()
    df_sort['2moDP']  = df.loc[df['CENRACE'].isin(cols_2mo)].groupby('GEOID')['GEOID'].count()
    df_sort  = df_sort.fillna(0)
    
    # each race nonhispanic alone 
    df_sort['NHwhi_aloDP']  = df.loc[(df['CENRACE'] == 1) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHbla_aloDP']  = df.loc[(df['CENRACE'] == 2) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHnat_aloDP']  = df.loc[(df['CENRACE'] == 3) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHasi_aloDP']  = df.loc[(df['CENRACE'] == 4) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHpci_aloDP']  = df.loc[(df['CENRACE'] == 5) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHsor_aloDP']  = df.loc[(df['CENRACE'] == 6) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NH2moDP']  = df.loc[(df['CENRACE'].isin(cols_2mo)) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort  = df_sort.fillna(0)
    
    # each race alone VAP only
    df_sort['WVAP_aloDP']  = df.loc[(df['CENRACE'] == 1) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    df_sort['BVAP_aloDP']  = df.loc[(df['CENRACE'] == 2) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    df_sort['natVAP_aloDP']  = df.loc[(df['CENRACE'] == 3) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    df_sort['AVAP_aloDP']  = df.loc[(df['CENRACE'] == 4) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    df_sort['pciVAP_aloDP']  = df.loc[(df['CENRACE'] == 5) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    df_sort['sorVAP_aloDP']  = df.loc[(df['CENRACE'] == 6) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    df_sort['2moVAPDP']  = df.loc[(df['CENRACE'].isin(cols_2mo)) & (df['VOTING_AGE'] == 2)].groupby('GEOID')['GEOID'].count()
    df_sort  = df_sort.fillna(0)
    
    # each race NH alone VAP only
    df_sort['NHWVAP_aloDP']  = df.loc[(df['CENRACE'] == 1) & (df['VOTING_AGE'] == 2) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHBVAP_aloDP']  = df.loc[(df['CENRACE'] == 2) & (df['VOTING_AGE'] == 2) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHnatVAP_aloDP']  = df.loc[(df['CENRACE'] == 3) & (df['VOTING_AGE'] == 2) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHAVAP_aloDP']  = df.loc[(df['CENRACE'] == 4) & (df['VOTING_AGE'] == 2) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHpciVAP_aloDP']  = df.loc[(df['CENRACE'] == 5) & (df['VOTING_AGE'] == 2) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NHsorVAP_aloDP']  = df.loc[(df['CENRACE'] == 6) & (df['VOTING_AGE'] == 2) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort['NH2moVAPDP']  = df.loc[(df['CENRACE'].isin(cols_2mo)) & (df['VOTING_AGE'] == 2) & (df['CENHISP'] == 1)].groupby('GEOID')['GEOID'].count()
    df_sort  = df_sort.fillna(0)


    # rename total pop variable
    df_sort.rename(columns={'GEOID':'totDP'}, inplace=True)
    
    return df_sort


#### SETUP

# set source data and chunk size
sheet4_5 = '/Users/ak6310/Desktop/ppmf_20210428_eps4-5_P.csv'
sheet12_2 = '/Users/ak6310/Desktop/ppmf_20210428_eps12-2_P.csv'

chunksize = 10 ** 6


#### STATE RUNS
    
# list states to run
state_fips = pd.read_csv('/Users/ak6310/Desktop/county_fips/state_fips.csv')

state_fips['fips2'] = state_fips['fips'].map(lambda x:str(x).zfill(2))
fips_list = list(state_fips['fips'])
abbrev_list = list(state_fips['abbrev'])

# set directory
cwd = os.getcwd()
cwd
os.chdir('/Users/ak6310/Desktop/Scratch')


# run for every state, write to file

for i in range(0, len(abbrev_list)):
    print (abbrev_list[i], fips_list[i])
    st4_5 = getStateData(fips_list[i], chunksize, sheet4_5)
    st12_2 = getStateData(fips_list[i], chunksize, sheet12_2)
    
    st4_5_sort = cleanData(st4_5)
    st12_2_sort = cleanData(st12_2)
    
    st4_5_sort.to_csv('./Census/CensusDP_{0}_4_5.csv'.format(abbrev_list[i]))
    st12_2_sort.to_csv('./Census/CensusDP_{0}_12_2.csv'.format(abbrev_list[i]))

