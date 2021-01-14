#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  7 09:52:28 2020

@author: Jim
"""
from databaker.framework import *
from databakerUtils.writers import v4Writer
import pandas as pd
import numpy as np
from databakerUtils.v4Functions import v4Integers
from databakerUtils.sparsityFunctions import SparsityFiller
from datetime import datetime
import requests

def Slugize(value):
    new_value = value.replace(' ', '-').replace(':', '').lower()
    return new_value

def ExcelDateChange(value):
    as_datetime = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    new_value = datetime.strftime(as_datetime, '%d-%b-%y')
    return new_value

def SexLabels(value):
    if value == None:
        return 'All'
    elif 'Person' in value:
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

url = 'https://api.beta.ons.gov.uk/v1/code-lists/administrative-geography/editions/one-off/codes'
whole_dict = requests.get(url, verify=False).json()
admin_dict = {}
for item in whole_dict['items']:
    admin_dict.update({item['code']:item['label']})
del whole_dict

def AdminGeogLabels(value):
    return admin_dict[value]

def TotalGeog(value):
    # Returns england & wales code for total
    if value.startswith('E'):
        return False
    elif value.startswith('W'):
        return False
    else:
        return True

def RemoveYTD(value):
    # returns false for any YTD figures
    if 'week' in value.lower():
        return False
    elif 'year to date' in value.lower():
        return False
    else:
        try:
            float(value)
            return True
        except:
            raise Exception('{} not a week number'.format(value))

def WeeklyDeathsByRegion(source_tabs):
    output_file = 'v4-weekly-deaths-regional.csv'
    
    tabs = source_tabs
    tabs = [tab for tab in tabs if 'Weekly' in tab.name]
    tabs = [tab for tab in tabs if 'UK' not in tab.name]
    
    year_of_data = tabs[0].name.split(' ')[-1]
    
    conversionsegments = []
    for tab in tabs:
        not_needed = tab.excel_ref('A').filter(contains_string('Foot')).expand(DOWN).expand(RIGHT)
        
        time = year_of_data
        
        geography = tab.excel_ref('A').filter(contains_string('E120')).expand(DOWN).is_not_blank().is_not_whitespace()
        geography -= not_needed
        geography |= tab.excel_ref('A9')
        
        week_number = tab.excel_ref('A').filter(contains_string('Week number')).fill(RIGHT).is_not_blank().is_not_whitespace()
        week_ended = week_number.shift(0, 1)
        
        death_type = tab.name
        
        obs = week_number.waffle(geography).is_not_blank().is_not_whitespace()
        
        dimensions = [
                HDimConst(TIME, time),
                HDim(geography, GEOG, DIRECTLY, LEFT),
                HDim(week_number, 'week_number', DIRECTLY, ABOVE),
                HDim(week_ended, 'week_ended', DIRECTLY, ABOVE),
                HDimConst('death_type', death_type)
                ]
        
        for cell in dimensions[3].hbagset:
            dimensions[3].AddCellValueOverride(cell, str(cell.value))
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
        
    data = pd.concat(conversionsegments)
    df = v4Writer('file-path', data, asFrame=True)
    
    '''Post Processing'''
    df['V4_0'] = df['V4_0'].apply(v4Integers)

    df.loc[df['Geography_codelist'].apply(TotalGeog), 'Geography_codelist'] = 'K04000001'
    df['Geography'] = df['Geography_codelist'].apply(AdminGeogLabels)
    
    # excluding the year to date figures
    df = df[df['week_number'].apply(RemoveYTD)]
    
    df['week_number'] = df['week_number'].apply(lambda x: str(int(float(x))))
    df['week_number_codelist'] = 'week-' + df['week_number']
    df['week_number'] = 'Week ' + df['week_number'].apply(WeekNumberLabels) #+ ' ending ' + df['week_ended'].apply(ExcelDateChange)
    
    df['death_type'] = df['death_type'].apply(DeathType)
    df['death_type_codelist'] = df['death_type'].apply(Slugize)
    
    df = df.drop(['week_ended', 'week_ended_codelist'], axis=1)
    
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
        
    PreviousWeekV4Checker(output_file, df)
    df.to_csv(output_file, index=False)

def WeeklyDeathsByAgeSex(source_tabs):
    output_file = 'v4-weekly-deaths-age-sex.csv'
    
    tabs = source_tabs
    tabs = [tab for tab in tabs if 'Weekly' in tab.name]
    tabs = [tab for tab in tabs if 'UK' not in tab.name]
    
    year_of_data = tabs[0].name.split(' ')[-1]
    
    conversionsegments = []
    for tab in tabs:
        regional_data = tab.excel_ref('A').filter(contains_string('E120')).expand(DOWN).expand(RIGHT)
    
        time = year_of_data
        
        geography = 'K04000001'
        
        week_number = tab.excel_ref('A').filter(contains_string('Week number')).fill(RIGHT).is_not_blank().is_not_whitespace()
        week_ended = week_number.shift(0, 1)
        
        sex = tab.excel_ref('B').filter(contains_string('Person')) |\
                tab.excel_ref('B').filter(contains_string('Male')) |\
                tab.excel_ref('B').filter(contains_string('Female'))
        
        age = tab.excel_ref('B').filter(contains_string('Person')).fill(DOWN).is_not_blank().is_not_whitespace()
        age -= sex | tab.excel_ref('B').filter(contains_string('Deaths')) | regional_data
        age |= tab.excel_ref('A9')
        
        death_type = tab.name
        
        obs = week_number.waffle(age).is_not_blank().is_not_whitespace()
        
        dimensions = [
                HDimConst(TIME, time),
                HDimConst(GEOG, geography),
                HDim(week_number, 'week_number', DIRECTLY, ABOVE),
                HDim(week_ended, 'week_ended', DIRECTLY, ABOVE),
                HDim(sex, 'sex', CLOSEST, ABOVE),
                HDim(age, 'age', DIRECTLY, LEFT),
                HDimConst('death_type', death_type)
                ]
        
        for cell in dimensions[3].hbagset:
            dimensions[3].AddCellValueOverride(cell, str(cell.value))
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
        
    data = pd.concat(conversionsegments)
    df = v4Writer('file-path', data, asFrame=True)
    
    '''Post Processing'''
    df['V4_0'] = df['V4_0'].apply(v4Integers)
    
    df['Geography'] = 'England and Wales'
    
    # excluding the year to date figures
    df = df[df['week_number'].apply(RemoveYTD)]
    
    df['week_number'] = df['week_number'].apply(lambda x: str(int(float(x))))
    df['week_number_codelist'] = 'week-' + df['week_number']
    df['week_number'] = 'Week ' + df['week_number'].apply(WeekNumberLabels) #+ ' ending ' + df['week_ended'].apply(ExcelDateChange)
    
    df['sex'] = df['sex'].apply(SexLabels)
    df['sex_codelist'] = df['sex'].apply(lambda x: x.lower())
    
    df['age'] = df['age'].apply(AgeLabels)
    df['age_codelist'] = df['age'].apply(AgeCodes)
    
    df['death_type'] = df['death_type'].apply(DeathType)
    df['death_type_codelist'] = df['death_type'].apply(Slugize)
    
    df = df.drop(['week_ended', 'week_ended_codelist'], axis=1)
    
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
    
    PreviousWeekV4Checker(output_file, df)
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, 'x')

def WeeklyDeathsByLA_HB(registration_tab, occurrence_tab, year):
    output_file_la = 'v4-weekly-deaths-local-authority.csv'
    output_file_hb = 'v4-weekly-deaths-health-board.csv'
    
    year_of_data = year
    
    reg_data = registration_tab.rename(columns=lambda x: x.strip().lower())
    occ_data = occurrence_tab.rename(columns=lambda x: x.strip().lower())
    
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

    df_la['Geography'] = df_la['administrative-geography'].apply(AdminGeogLabels)
    
    PreviousWeekV4Checker(output_file_hb, df_hb)
    df_hb.to_csv(output_file_hb, index=False)
    SparsityFiller(output_file_hb)
    
    PreviousWeekV4Checker(output_file_la, df_la)
    df_la.to_csv(output_file_la, index=False)
    SparsityFiller(output_file_la)
    
def PreviousWeekV4Checker(file, new_df):
    all_ok = True
    
    df = pd.read_csv(file, dtype=str)
    
    print('Quick comparison check on {}'.format(file.split('/')[-1]))
    
    # checking both columns are the same -> ignoring v4 and data marking cols
    df_cols = [col for col in df.columns if '4' not in col and 'Data Marking' not in col]
    new_df_cols = [col for col in new_df.columns if '4' not in col and 'Data Marking' not in col]
    
    if df_cols != new_df_cols:
        raise Exception('Columns for {} do not match previous v4 columns'.format(file.split('/'[-1])))
        
    # check week number has increased by 1
    max_week_number = int(df['week-number'].unique()[-1].split('-')[-1])
    new_max_week_number = int(new_df['week-number'].unique()[-1].split('-')[-1])
    
    if new_max_week_number - max_week_number != 1:
        if new_max_week_number == 1:
            print('New v4 looks like week 1 of data')
            if df['Time'].unique()[0] == new_df['Time'].unique()[0]:
                print('Looks like you need to update the year')
                all_ok = False
        else:
            print(
                'Difference between week numbers is not correct\n'
                'Previous week goes up to {}\n'
                'New week goes up to {}'.format(max_week_number, new_max_week_number)
                    )
            all_ok = False
    
    # check on the year of data
    if df['Time'].unique()[0] != new_df['Time'].unique()[0]:
        print('Year of data does not match.. okay if this is week 1')
        
    # check the length of dimensions -> excluding week number
    for col in [col for col in df_cols][1::2]: # all 'code' columns except week number
        if 'week' in col:
            continue
        if df[col].unique().size != new_df[col].unique().size:
            if df[col].unique().size > new_df[col].unique().size:
                print('Previous v4 has more options in {}'.format(col))
            else:
                print('New v4 has more options in {}'.format(col))
            all_ok = False
            
    # check length of v4's
    if len(new_df) < len(df):
        if new_max_week_number != 1:
            unsparse_length = np.prod([new_df[col].unique().size for col in new_df_cols][::2]) 
            # ^ length of new df without sparsity
            if unsparse_length < len(df):
                print('New v4 is not longer than previous v4')
                all_ok = False
            elif unsparse_length == len(df):
                print('New and previous v4 have same length, are week numbers the same')
                all_ok = False
    elif len(new_df) == len(df):
                print('New and previous v4 have same length, are week numbers the same')
                all_ok = False
            
    if all_ok:
        print('All ok')
        
            

    























