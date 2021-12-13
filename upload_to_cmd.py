from api_pipeline import Multi_Upload_To_Cmd
import WDFunctions as w

upload_dict = {
        'weekly-deaths-health-board':{
                'v4':'D:/v4-weekly-deaths-health-board-{}.csv'.format(year_of_data),
                'edition':year_of_data,
                'collection_name':'CMD weekly deaths by health board',
                'metadata_file':'D:/weekly-deaths-health-board-2021-v26.csv-metadata.json'
                },
        'weekly-deaths-region':{
                'v4':'D:/v4-weekly-deaths-regional.csv',
                'edition':'covid-19',
                'collection_name':'CMD weekly deaths by region',
                'metadata_file':'D:/weekly-deaths-region-covid-19-v39.csv-metadata.json'
                },
        'weekly-deaths-age-sex':{
                'v4':'D:/v4-weekly-deaths-age-sex.csv',
                'edition':'covid-19',
                'collection_name':'CMD weekly deaths by age and sex',
                'metadata_file':'D:/weekly-deaths-age-sex-covid-19-v39.csv-metadata.json'
                },
        'weekly-deaths-local-authority':{
                'v4':'D:/v4-weekly-deaths-local-authority-{}.csv'.format(year_of_data),
                'edition':year_of_data,
                'collection_name':'CMD weekly deaths by local authority',
                'metadata_file':'D:/weekly-deaths-local-authority-2021-v26.csv-metadata.json'
                }
        }

for dataset in upload_dict:
    # check date on v4
    v4 = upload_dict[dataset]['v4']
    w.Check_v4_Date(v4)
    
Multi_Upload_To_Cmd(credentials, upload_dict)