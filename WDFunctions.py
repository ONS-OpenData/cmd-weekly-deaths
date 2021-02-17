#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  7 09:52:28 2020

@author: Jim
"""
from databaker.framework import *
from databakerUtils.writers import v4Writer
import pandas as pd
from databakerUtils.v4Functions import v4Integers
from databakerUtils.sparsityFunctions import SparsityFiller

def Slugize(value):
    new_value = value.replace(' ', '-').replace(':', '').lower()
    return new_value

def SexLabels(value):
    if value == None:
        return 'All'
    elif 'Person' in value:
        return 'All'
    elif 'People' in value:
        return 'All'
    elif 'Female' in value:
        return 'Female'
    elif 'Male' in value:
        return 'Male'

def AgeLabels(value):
    if 'Under 1' in value or '<1' in value:
        return '00-01'
    elif value == '1-4':
        return '01-04'
    elif value == '5-9':
        return '05-09'
    elif 'all ages' in value.lower():
        return 'All ages'
    else:
        return value

def AgeCodes(value):
    lookup = {
            '00-01':'0-1',
            '01-04':'1-4',
            '05-09':'5-9',
            'All ages':'all-ages'
            }
    return lookup.get(value, value)

def WeekNumberLabels(value):
    value = str(value)
    as_int = int(value)
    if as_int < 10:
        new_value = '0' + value
        return new_value
    else:
        return value

def DeathType(value):
    if 'registrations' in value.lower():
        return 'Deaths involving COVID-19: registrations'
    elif 'occurrences' in value.lower():
        return 'Deaths involving COVID-19: occurrences'
    else:
        return 'Total registered deaths'

def TotalGeog(value):
    # Returns england & wales code for total
    if value.startswith('E'):
        return False
    elif value.startswith('W'):
        return False
    else:
        return True

def WeeklyDeathsByRegion(source_tabs):
    file = 'v4-weekly-deaths-regional.csv'
    
    tabs = source_tabs
    tabs = [tab for tab in tabs if 'Weekly' in tab.name]
    tabs = [tab for tab in tabs if 'UK' not in tab.name]
    tabs = [tab for tab in tabs if 'cause' not in tab.name.lower()]
    
    year_of_data = tabs[0].name.split(' ')[-1]
    
    conversionsegments = []
    for tab in tabs:
        not_needed = tab.excel_ref('A').filter(contains_string('Footnotes:')).expand(DOWN).expand(RIGHT)
        
        time = year_of_data
        
        geography = tab.excel_ref('A').filter(contains_string('E120')).expand(DOWN).is_not_blank()
        geography -= not_needed
        geography |= tab.excel_ref('A9')
        
        geography_labels = geography.shift(1, 0)
        
        week_number = tab.excel_ref('A').filter(contains_string('Week number')).fill(RIGHT).is_not_blank()
        # strange data marking in spreadsheet for week 53 2021
        unwanted_week_number = week_number.filter(contains_string('53 '))
        if 'occurrences' in tab.name:
            unwanted_week_number |= tab.excel_ref('BC5').expand(LEFT)
            assert tab.excel_ref('BC5').value == 53, 'Data in occurences tab has moved'
        week_number -= unwanted_week_number
        
        death_type = tab.name
        
        obs = week_number.waffle(geography).is_not_blank()
        
        dimensions = [
                HDimConst(TIME, time),
                HDim(geography, GEOG, DIRECTLY, LEFT),
                HDim(geography_labels, 'geography_labels', DIRECTLY, LEFT),
                HDim(week_number, 'week_number', DIRECTLY, ABOVE),
                HDimConst('death_type', death_type)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
        
    data = pd.concat(conversionsegments)
    df = v4Writer('file-path', data, asFrame=True)
    
    '''Post Processing'''
    df['V4_0'] = df['V4_0'].apply(v4Integers)

    # sorting geography
    df.loc[df['Geography_codelist'].apply(TotalGeog), 'Geography_codelist'] = 'K04000001'
    df.loc[df['geography_labels'] == '', 'geography_labels'] = 'England and Wales'
    df['Geography'] = df['geography_labels']
    df = df.drop(['geography_labels', 'geography_labels_codelist'], axis=1)
    
    # excluding the year to date figures
    df = df[df['week_number'].apply(lambda x: 'week' not in x.lower())].reset_index(drop=True)
    
    # sorting week number
    df['week_number'] = df['week_number'].apply(lambda x: str(int(float(x))))
    df['week_number_codelist'] = 'week-' + df['week_number']
    df['week_number'] = 'Week ' + df['week_number'].apply(WeekNumberLabels) 
    
    df['death_type'] = df['death_type'].apply(DeathType)
    df['death_type_codelist'] = df['death_type'].apply(Slugize)
    
    df = df.rename(columns = {
            'Time_codelist':'calendar-years',
            'Time':'Time',
            'Geography_codelist':'administrative-geography',
            'Geography':'Geography',
            'week_number_codelist':'week-number',
            'week_number':'Week',
            'death_type_codelist':'recorded-deaths',
            'death_type':'Deaths'
            }
        )
    
    # v4 needs to be added to previous weeks v4
    # only the latest weeks data to be added
    latest_week_number = df[df['Deaths'] == 'Total registered deaths']
    latest_week_number = latest_week_number['week-number'].unique()[-1]
    
    df = df[df['week-number'] == latest_week_number]
    
    df['Data Marking'] = None
    df = df.rename(columns={'V4_0':'V4_1'})
    df = df[['V4_1', 'Data Marking', 'calendar-years', 'Time', 'administrative-geography',
       'Geography', 'week-number', 'Week', 'recorded-deaths', 'Deaths']]
    
    # read in previous v4
    previous_df = pd.read_csv(file, dtype=str)
    previous_df = previous_df[previous_df['Data Marking'] != 'x'] # removes any previous filled from SparsityFiller

    new_df = pd.concat([previous_df, df]).drop_duplicates()
    
    V4Checker(new_df, 'region')
    new_df.to_csv(file, index=False)
    SparsityFiller(file, 'x')

def WeeklyDeathsByAgeSex(source_tabs):
    file = 'v4-weekly-deaths-age-sex.csv'
    
    tabs = source_tabs
    tabs = [tab for tab in tabs if 'Weekly' in tab.name]
    tabs = [tab for tab in tabs if 'UK' not in tab.name]
    tabs = [tab for tab in tabs if 'cause' not in tab.name.lower()]
    
    year_of_data = tabs[0].name.split(' ')[-1]
    
    conversionsegments = []
    for tab in tabs:
        regional_data = tab.excel_ref('A').filter(contains_string('E120')).expand(DOWN).expand(RIGHT)
    
        time = year_of_data
        
        geography = 'K04000001'
        
        week_number = tab.excel_ref('A').filter(contains_string('Week number')).fill(RIGHT).is_not_blank()
        # strange data marking in spreadsheet for week 53 2021
        unwanted_week_number = week_number.filter(contains_string('53 '))
        if 'occurrences' in tab.name:
            unwanted_week_number |= tab.excel_ref('BC5').expand(LEFT)
            assert tab.excel_ref('BC5').value == 53, 'Data in occurences tab has moved'
        week_number -= unwanted_week_number
        
        sex = tab.excel_ref('B').filter(contains_string('Person')) |\
                tab.excel_ref('B').filter(contains_string('People')) |\
                tab.excel_ref('B').filter(contains_string('Male')) |\
                tab.excel_ref('B').filter(contains_string('Female'))
                
        assert len(sex) == 3, 'Sex labels have moved in tab {}'.format(tab.name)
        
        age = tab.excel_ref('B').filter(contains_string('Person')).fill(DOWN).is_not_blank()
        if len(age) == 0:
            age = tab.excel_ref('B').filter(contains_string('People')).fill(DOWN).is_not_blank()
        age -= sex | tab.excel_ref('B').filter(contains_string('Deaths')) | regional_data
        age |= tab.excel_ref('A9')
        
        death_type = tab.name
        
        obs = week_number.waffle(age).is_not_blank()
        
        dimensions = [
                HDimConst(TIME, time),
                HDimConst(GEOG, geography),
                HDim(week_number, 'week_number', DIRECTLY, ABOVE),
                HDim(sex, 'sex', CLOSEST, ABOVE),
                HDim(age, 'age', DIRECTLY, LEFT),
                HDimConst('death_type', death_type)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
        
    data = pd.concat(conversionsegments)
    df = v4Writer('file-path', data, asFrame=True)
    
    '''Post Processing'''
    df['V4_0'] = df['V4_0'].apply(v4Integers)
    
    df['Geography'] = 'England and Wales'
    
    # excluding the year to date figures
    df = df[df['week_number'].apply(lambda x: 'week' not in x.lower())].reset_index(drop=True)
    
    # sorting week number
    df['week_number'] = df['week_number'].apply(lambda x: str(int(float(x))))
    df['week_number_codelist'] = 'week-' + df['week_number']
    df['week_number'] = 'Week ' + df['week_number'].apply(WeekNumberLabels) 
    
    df['sex'] = df['sex'].apply(SexLabels)
    df['sex_codelist'] = df['sex'].apply(lambda x: x.lower())
    
    df['age'] = df['age'].apply(AgeLabels)
    df['age_codelist'] = df['age'].apply(AgeCodes)
    
    df['death_type'] = df['death_type'].apply(DeathType)
    df['death_type_codelist'] = df['death_type'].apply(Slugize)
    
    df = df.rename(columns = {
            'Time_codelist':'calendar-years',
            'Time':'Time',
            'Geography_codelist':'administrative-geography',
            'Geography':'Geography',
            'week_number_codelist':'week-number',
            'week_number':'Week',
            'sex':'Sex',
            'sex_codelist':'sex',
            'age_codelist':'age-groups',
            'age':'AgeGroups',
            'death_type_codelist':'recorded-deaths',
            'death_type':'Deaths'
            }
        )
    
    
    
    # v4 needs to be added to previous weeks v4
    # only the latest weeks data to be added
    latest_week_number = df[df['Deaths'] == 'Total registered deaths']
    latest_week_number = latest_week_number['week-number'].unique()[-1]
    
    df = df[df['week-number'] == latest_week_number]
    
    df['Data Marking'] = None
    df = df.rename(columns={'V4_0':'V4_1'})
    df = df[['V4_1', 'Data Marking', 'calendar-years', 'Time', 'administrative-geography',
       'Geography', 'week-number', 'Week', 'sex', 'Sex', 'age-groups',
       'AgeGroups', 'recorded-deaths', 'Deaths']]
    
    # read in previous v4
    previous_df = pd.read_csv(file, dtype=str)
    previous_df = previous_df[previous_df['Data Marking'] != 'x'] # removes any previous filled from SparsityFiller

    new_df = pd.concat([previous_df, df]).drop_duplicates()
    
    V4Checker(new_df, 'age&sex')
    new_df.to_csv(file, index=False)
    SparsityFiller(file, 'x')


def WeeklyDeathsByLA_HB(registration_tabs, occurrence_tabs, year):
    output_file_la = 'v4-weekly-deaths-local-authority-{}.csv'.format(year)
    output_file_hb = 'v4-weekly-deaths-health-board-{}.csv'.format(year)
    
    year_of_data = year
    
    reg_data = registration_tabs.rename(columns=lambda x: x.strip().lower())
    occ_data = occurrence_tabs.rename(columns=lambda x: x.strip().lower())
    
    #add registration or occurrence
    reg_data['registrationoroccurrence'] = 'Registrations'
    occ_data['registrationoroccurrence'] = 'Occurrences'
    
    df = pd.concat([reg_data, occ_data])
    
    df['calendar-years'] = year_of_data
    df['time'] = df['calendar-years']
    
    df['cause-of-death'] = df['cause of death'].apply(Slugize)
    df['place-of-death'] = df['place of death'].apply(Slugize)
    df['registration-or-occurrence'] = df['registrationoroccurrence'].apply(Slugize)
    
    df['week-number'] = df['week number'].apply(lambda x: 'week-' + str(x))
    df['week number'] = 'Week ' + df['week number'].apply(WeekNumberLabels)
    
    df = df.rename(columns={
            'number of deaths':'v4_0',
            'time':'Time',
            'cause of death':'CauseOfDeath',
            'place of death':'PlaceOfDeath',
            'week number':'Week',
            'area name':'Geography',
            'registrationoroccurrence':'RegistrationOrOccurrence'
            }
        )
    
    df = df[[
            'v4_0', 'calendar-years', 'Time', 'area code', 'Geography', 'geography type', 
            'week-number', 'Week', 'cause-of-death', 'CauseOfDeath', 'place-of-death', 'PlaceOfDeath',
            'registration-or-occurrence', 'RegistrationOrOccurrence'
            ]]
    
    df_hb = df[df['geography type'] != 'Local Authority'].drop(['geography type'], axis=1).rename(columns={
            'area code':'local-health-board'
            }
    )
    df_la = df[df['geography type'] == 'Local Authority'].drop(['geography type'], axis=1).rename(columns={
            'area code':'administrative-geography'
            }
    )
    
    V4Checker(df_hb, 'health-board')
    df_hb.to_csv(output_file_hb, index=False)
    SparsityFiller(output_file_hb)
    
    V4Checker(df_la, 'local-authority')
    df_la.to_csv(output_file_la, index=False)
    SparsityFiller(output_file_la)
    
    
def V4Checker(v4, dataset):
    '''
    Checks the dimensions of the v4 to make sure no irregularities
    '''
    df = v4.copy()
    # obs and data marking column not needed
    df = df[[col for col in df.columns if '4' not in col and 'Data Marking' not in col]]
    
    if dataset == 'region':
        for col in df.columns:
            if col not in ('calendar-years', 'Time', 'administrative-geography', 'Geography',
                           'week-number', 'Week', 'recorded-deaths', 'Deaths'):
                raise Exception('V4Checker on {} data - "{}" is not a correct column'.format(dataset, col))
        
        # year check
        for code in df['Time'].unique():
            try: 
                int(code)
            except:
                raise Exception('V4Checker on {} data - "{}" is not a year'.format(dataset, code))
            
            if int(code) < 2020:
                raise Exception('V4Checker on {} data - "{}" is outside of year range, data started in 2020'.format(dataset, code))
        
        # geography check
        for code in df['administrative-geography'].unique():
            if code not in ('E12000004', 'E12000007', 'W92000004', 'E12000009', 'E12000003',
                            'E12000008', 'K04000001', 'E12000002', 'E12000005', 'E12000006',
                            'E12000001'):
                raise Exception('V4Checker on {} data - "{}" should not be in geography codes'.format(dataset, code))
                
        # week number check
        for code in df['week-number'].unique():
            week_number = int(code.split('-')[-1])
            if week_number > 53:
                raise Exception('V4Checker on {} data - week "{}" is out of range'.format(dataset, code))
                
        # recorded death check
        for code in df['recorded-deaths'].unique():
            if code not in ('deaths-involving-covid-19-registrations',
                            'deaths-involving-covid-19-occurrences', 
                            'total-registered-deaths'):
                raise Exception('V4Checker on {} data - "{}" should not be in recorded deaths'.format(dataset, code))
                
    
    elif dataset == 'age&sex':
        for col in df.columns:
            if col not in ('calendar-years', 'Time', 'administrative-geography', 'Geography', 
                           'week-number', 'Week', 'sex', 'Sex', 'age-groups', 'AgeGroups', 
                           'recorded-deaths', 'Deaths'):
                raise Exception('V4Checker on {} data - "{}" is not a correct column'.format(dataset, col))
        
        # year check
        for code in df['Time'].unique():
            try: 
                int(code)
            except:
                raise Exception('V4Checker on {} data - "{}" is not a year'.format(dataset, code))
            
            if int(code) < 2020:
                raise Exception('V4Checker on {} data - "{}" is outside of year range, data started in 2020'.format(dataset, code))
        
        # geography check
        for code in df['administrative-geography'].unique():
            if code not in ('K04000001'):
                raise Exception('V4Checker on {} data - "{}" should not be in geography codes'.format(dataset, code))
              
        # week number check
        for code in df['week-number'].unique():
            week_number = int(code.split('-')[-1])
            if week_number > 53:
                raise Exception('V4Checker on {} data - week "{}" is out of range'.format(dataset, code))
                
        # recorded death check
        for code in df['recorded-deaths'].unique():
            if code not in ('deaths-involving-covid-19-registrations',
                            'deaths-involving-covid-19-occurrences', 
                            'total-registered-deaths'):
                raise Exception('V4Checker on {} data - "{}" should not be in recorded deaths'.format(dataset, code))
                
        # sex codes check
        for code in df['sex'].unique():
            if code not in ('male', 'female', 'all'):
                raise Exception('V4Checker on {} data - "{}" should not be in sex codes'.format(dataset, code))
                
        # age groups check
        for code in df['age-groups'].unique():
            if code not in ('90+', '20-24', '15-19', '85-89', '1-4', '0-1', 'all-ages',
                           '45-49', '40-44', '30-34', '65-69', '70-74', '75-79', '5-9',
                           '25-29', '50-54', '55-59', '80-84', '60-64', '10-14', '35-39'):
                raise Exception('V4Checker on {} data - "{}" should not be in age groups'.format(dataset, code))
    
    
    elif dataset.lower().replace('-', ' ') in ('la', 'local authority'):
        for col in df.columns:
            if col not in ('calendar-years', 'Time', 'administrative-geography', 'Geography',
                           'week-number', 'Week', 'cause-of-death', 'CauseOfDeath',
                           'place-of-death', 'PlaceOfDeath', 'registration-or-occurrence',
                           'RegistrationOrOccurrence'):
                raise Exception('V4Checker on {} data - "{}" is not a correct column'.format(dataset, col))
        
        # year checker - only one year of data per edition
        assert df['Time'].unique().size == 1, 'V4Checker on {} data - should only have one option for time but has {}'.format(dataset, df['Time'].unique().size)
        
        # geography check - dont want to call api and too many codes to create a list
        # quick check by counting codes - a change would mean sparsity anyway
        assert df['administrative-geography'].unique().size == 336, 'V4Checker on {} data - been a change to the number of geographies, should be 336 but there is {}'.format(dataset, df['administrative-geography'].unique().size)
        
        # week number check
        for code in df['week-number'].unique():
            week_number = int(code.split('-')[-1])
            if week_number > 53:
                raise Exception('V4Checker on {} data - week "{}" is out of range'.format(dataset, code))
         
        # cause of death check
        for code in df['cause-of-death'].unique():
            if code not in ('all-causes', 'covid-19'):
                raise Exception('V4Checker on {} data - "{}" should not be in cause of death'.format(dataset, code))
                
        # place of death check
        for code in df['place-of-death'].unique():
            if code not in ('care-home', 'elsewhere', 'home', 'hospice', 'hospital',
                            'other-communal-establishment'):
                raise Exception('V4Checker on {} data - "{}" should not be in place of death'.format(dataset, code))
                
        # registration or occurrence check - hard coded in transform
        # so just a quick check that they have the same number
        if len(df[df['registration-or-occurrence'] == 'registrations']) != len(df[df['registration-or-occurrence'] == 'occurrences']):
            raise Exception('V4Checker on {} data - there are a different number of registrations and occurences'.format(dataset))
        
        
    elif dataset.lower().replace('-', ' ') in ('hb', 'health board'):
        for col in df.columns:
            if col not in ('calendar-years', 'Time', 'local-health-board', 'Geography',
                           'week-number', 'Week', 'cause-of-death', 'CauseOfDeath',
                           'place-of-death', 'PlaceOfDeath', 'registration-or-occurrence',
                           'RegistrationOrOccurrence'):
                raise Exception('V4Checker on {} data - "{}" is not a correct column'.format(dataset, col))
        
        # year checker - only one year of data per edition
        assert df['Time'].unique().size == 1, 'V4Checker on {} data - should only have one option for time but has {}'.format(dataset, df['Time'].unique().size)
        
        # geography check 
        for code in df['local-health-board'].unique():
            if code not in ('W11000023', 'W11000024', 'W11000025', 'W11000028', 'W11000029',
                            'W11000030', 'W11000031'):
                raise Exception('V4Checker on {} data - "{}" should not be in local health board'.format(dataset, code))
        
        # week number check
        for code in df['week-number'].unique():
            week_number = int(code.split('-')[-1])
            if week_number > 53:
                raise Exception('V4Checker on {} data - week "{}" is out of range'.format(dataset, code))
         
        # cause of death check
        for code in df['cause-of-death'].unique():
            if code not in ('all-causes', 'covid-19'):
                raise Exception('V4Checker on {} data - "{}" should not be in cause of death'.format(dataset, code))
                
        # place of death check
        for code in df['place-of-death'].unique():
            if code not in ('care-home', 'elsewhere', 'home', 'hospice', 'hospital',
                            'other-communal-establishment'):
                raise Exception('V4Checker on {} data - "{}" should not be in place of death'.format(dataset, code))
                
        # registration or occurrence check - hard coded in transform
        # so just a quick check that they have the same number
        if len(df[df['registration-or-occurrence'] == 'registrations']) != len(df[df['registration-or-occurrence'] == 'occurrences']):
            raise Exception('V4Checker on {} data - there are a different number of registrations and occurences'.format(dataset))
        
    print('{} is ok'.format(dataset))























