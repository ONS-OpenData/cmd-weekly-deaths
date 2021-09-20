import WDFunctions as w
import glob
from api_pipeline import Multi_Upload_To_Cmd

credentials = 'florence-details.json'

year_of_data = '2021'

location = 'weekly-deaths/*' # path name to directory of files, * for current directory
files = glob.glob('*')

file = [file for file in files if 'published' in file.lower()][0] #'publishedweek202020.xlsx'
tabs = w.loadxlstabs(file)
w.WeeklyDeathsByRegion(tabs)
w.WeeklyDeathsByAgeSex(tabs)

file = [file for file in files if 'lahb' in file] # 'lahbtablesweek20finalcodes.xlsx'
if len(file) == 0: 
    file = [file for file in files if 'la_hb' in file.lower()][0]
else: file = file[0]
reg_data = w.pd.read_excel(file, sheet_name='Registrations - All data', skiprows=3)
occ_data = w.pd.read_excel(file, sheet_name='Occurrences - All data', skiprows=3)
w.WeeklyDeathsByLA_HB(reg_data, occ_data, year_of_data)

print('Transforms complete!')

# required info for cmd upload
upload_dict = {
        'weekly-deaths-health-board':{
                'v4':'D:/v4-weekly-deaths-health-board-{}.csv'.format(year_of_data),
                'edition':year_of_data,
                'collection_name':'CMD weekly deaths by health board',
                'metadata_file':'weekly-deaths/weekly-deaths-health-board-2021-v26.csv-metadata.json'
                },
        'weekly-deaths-region':{
                'v4':'D:/v4-weekly-deaths-regional.csv',
                'edition':'covid-19',
                'collection_name':'CMD weekly deaths by region',
                'metadata_file':'weekly-deaths/weekly-deaths-region-covid-19-v39.csv-metadata.json'
                },
        'weekly-deaths-age-sex':{
                'v4':'D:/v4-weekly-deaths-age-sex.csv',
                'edition':'covid-19',
                'collection_name':'CMD weekly deaths by age and sex',
                'metadata_file':'weekly-deaths/weekly-deaths-age-sex-covid-19-v39.csv-metadata.json'
                },
        'weekly-deaths-local-authority':{
                'v4':'D:/v4-weekly-deaths-local-authority-{}.csv'.format(year_of_data),
                'edition':year_of_data,
                'collection_name':'CMD weekly deaths by local authority',
                'metadata_file':'weekly-deaths/weekly-deaths-local-authority-2021-v26.csv-metadata.json'
                }
        }

Multi_Upload_To_Cmd(credentials, upload_dict)