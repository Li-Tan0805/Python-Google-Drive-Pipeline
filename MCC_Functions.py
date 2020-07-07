
# coding: utf-8

# In[49]:


from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import pandas as pd
import numpy as np
from datetime import datetime
import glob
import yagmail
from IPython.display import clear_output, display


# In[61]:


# Self-defined

def folder_retrieval(folder_id, drive):
    query = "'" + folder_id + "'" + ' in parents and trashed = false'
    content_list = drive.ListFile({'q': query}).GetList()
    lst = []
    col = ['name','id','type']
    for content in content_list:
        lst.append([content['title'],content['id'],content['mimeType']])
    result = pd.DataFrame(lst, columns = col)
    return (result)

def file_reader(file_name):
    data = pd.read_excel(file_name,
                        sheet_name = 'Plan',
                        skiprows = 5,
                        index_col = None)
    try:
        data = data[data.Geo == 'US']
        data['Buy Details'] = file_name
    except: 
        data = 'Empty'
    return (data)

def file_combine(file_list):
    for i,each_file in enumerate(file_list):
        clear_output(wait = True)
        print('Combining ' + each_file)
        try:
            if i == 0:
                master = file_reader(each_file)
                cols = master.columns
            else:
                temp = file_reader(each_file)
                cols = temp.columns
                master = pd.concat([master, temp], ignore_index=True)
                master = master[cols]
        except:
            pass
    return(master)

def file_reader_cross_quarter(file_name):
    data = pd.read_excel(file_name,
                        sheet_name = 'Plan',
                        skiprows = 17,
                        index_col = None)
    try:
        data = data[data.Geo == 'US']
        data['Buy Details'] = file_name
    except: 
        data = 'Empty'
    return (data)

def file_combine_cross_quarter(file_list):
    for i,each_file in enumerate(file_list):
        clear_output(wait = True)
        print('Combining ' + each_file)
        try:
            if i == 0:
                master = file_reader_cross_quarter(each_file)
                cols = master.columns
            else:
                temp = file_reader(each_file)
                cols = temp.columns
                master = pd.concat([master, temp], ignore_index=True)
                master = master[cols]
        except:
            pass
    return(master)


# In[119]:


# Clorox AMJ

def Clorox_AMJ_BD_download(Clorox_BD_ID, Amazon_Cross_Quarter, file_save_location, output_location, drive):
    
    file_save_location_true = file_save_location.replace('[\]','[\\]')
    output_location_true = output_location.replace('[\]','[\\]')
    Clorox_BD_ID = Clorox_BD_ID
    Amazon_Cross_Quarter = Amazon_Cross_Quarter
    
    clorox_list = folder_retrieval(Clorox_BD_ID, drive)
    AMJ_ID = clorox_list[clorox_list.name == '4. AMJ FY20'].id.to_string(index = False).strip()
    AMJ_list = folder_retrieval(AMJ_ID, drive)

    Amazon_ID = clorox_list[clorox_list.name == 'Amazon'].id.to_string(index = False).strip()
    Amazon_list = folder_retrieval(Amazon_ID, drive)
    Amazon_AMJ = Amazon_list[Amazon_list.name == 'AMJ'].id.to_string(index = False).strip()
    Amazon_Cross_Quarter = Amazon_list[Amazon_list.name == 'Cross-Quarter'].id.to_string(index = False).strip()
    Amazon_AMJ_list = folder_retrieval(Amazon_AMJ, drive)
    Amazon_Cross_Quarter_list = folder_retrieval(Amazon_Cross_Quarter, drive)
    
    File_Name = []
    Brand = []
    for i in range(len(AMJ_list)):
        brand_id = AMJ_list.id.iloc[i]
        files = folder_retrieval(brand_id, drive)
        if len(files)>0:
            files = files[files.name != 'Archive']
            files = files[files.name != 'Archived']
            for j in range(len(files)):
                if files.name.iloc[j] != 'Untitled spreadsheet':
                    File_Name.append(files.name.iloc[j])
                    Brand.append(AMJ_list.name.iloc[i])
                    file_id = files.id.iloc[j]
                    temp = drive.CreateFile({'id': file_id})
                    clear_output(wait = True)
                    print('Downloading file:  %s' % files.name.iloc[j]) 
                    temp.GetContentFile(files.name.iloc[j])

    for i in range(len(Amazon_AMJ_list)):
        brand_id = Amazon_AMJ_list.id.iloc[i]
        files = folder_retrieval(brand_id, drive)
        if len(files)>0:
            files = files[files.name != 'Archive']
            files = files[files.name != 'Archived']
            for j in range(len(files)):
                if files.name.iloc[j] != 'Untitled spreadsheet':
                    File_Name.append(files.name.iloc[j])
                    Brand.append('Amazon')
                    file_id = files.id.iloc[j]
                    temp = drive.CreateFile({'id': file_id})
                    clear_output(wait = True)
                    print('Downloading file:  %s' % files.name.iloc[j]) 
                    temp.GetContentFile(files.name.iloc[j])

    for i in range(len(Amazon_Cross_Quarter_list)):
        brand_id = Amazon_Cross_Quarter_list.id[i]
        files = folder_retrieval(brand_id, drive)
        if len(files)>0:
            files = files[files.name.str.lower() != 'archive']
            files = files[files.name.str.lower() != 'archived']
            for j in range(len(files)):
                if files.name.iloc[j] != 'Untitled spreadsheet':
                    if 'folder' in files.type.iloc[j]:
                        folder_id = files.id.iloc[j]
                        sub_files = folder_retrieval(folder_id, drive)
                        if len(sub_files)>0:
                            sub_files = sub_files[sub_files.name.str.lower() != 'archive']
                            sub_files = sub_files[sub_files.name.str.lower() != 'archived']
                            for k in range(len(sub_files)):
                                if sub_files.name.iloc[k] != 'Untitled spreadsheet':
                                    if 'folder' in sub_files.type.iloc[k]:
                                        folder_id_2 = sub_files.id.iloc[k]
                                        sub_files_2 = folder_retrieval(folder_id_2, drive)
                                        if len(sub_files)>0:
                                            sub_files_2 = sub_files_2[sub_files_2.name.str.lower() != 'archive']
                                            sub_files_2 = sub_files_2[sub_files_2.name.str.lower() != 'archived']
                                            for l in range(len(sub_files_2)):
                                                File_Name.append(sub_files_2.name.iloc[l])
                                                Brand.append('Amazon Cross Quarter')
                                                sub_file_2_id = sub_files_2.id.iloc[l]
                                                temp = drive.CreateFile({'id': sub_file_2_id})
                                                clear_output(wait=True)
                                                print('Downloading file:  %s' % sub_files_2.name.iloc[l]) 
                                                temp.GetContentFile(sub_files_2.name.iloc[l])
                                    else:
                                        File_Name.append(sub_files.name.iloc[k])
                                        Brand.append('Amazon Cross Quarter')
                                        sub_file_id = sub_files.id.iloc[k]
                                        temp = drive.CreateFile({'id': sub_file_id})
                                        clear_output(wait=True)
                                        print('Downloading file:  %s' % sub_files.name.iloc[k]) 
                                        temp.GetContentFile(sub_files.name.iloc[k])
                    else:
                        File_Name.append(files.name.iloc[j])
                        Brand.append('Amazon Cross Quarter')
                        file_id = files.id.iloc[j]
                        temp = drive.CreateFile({'id': file_id})
                        clear_output(wait=True)
                        print('Downloading file:  %s' % files.name.iloc[j]) 
                        temp.GetContentFile(files.name.iloc[j])

    Reference = pd.DataFrame({'file name': File_Name, 'brand': Brand})
    Reference.to_csv('Reference.csv', index = False)
    
    file_list = glob.glob('*.xlsm')
    master = file_combine(file_list)
    
    reference = pd.read_csv('Reference.csv')
    reference.rename(columns={'file name':'Buy Details', 'brand':'Brand'}, inplace=True)
    
    clear_output(wait=True)
    print('Cleaning up data......')
    master_copy = master
    master = master[master['Geo'] != '']
    master = master.merge(reference)
    
    data = master
    Size = []
    for i in range(len(data)):
        if data['Placement Type'][i] == 'Package':
            Size.append('PKG')
        elif data['Placement Type'][i] == 'PKG':
            Size.append('PKG')
        elif ((data['Width'][i] == 'Vast') | (data['Width'][i] == 'VAST')):
            Size.append('0 x 0') 
        else:
            Size.append((str(data['Width'][i]) + ' x ' + str(data['Height'][i])).split('.')[0])
    data['Size'] = Size
    
    columns_needed = data.loc[:,['Line Item', 'Geo','Site Name','audience + placement name','Vehicle','Cost Structure',
                            'Campaign ID','Inventory Source','Targetin WHO','Size','Site Served or Dart']]
    data['DCM Placement Name'] = columns_needed.apply(lambda x: '|'.join(x.astype(str).values), axis=1)
    data['DCM Placement Name'].replace({'\.0': ''}, inplace=True, regex=True)
    
    data['Note - Missing Campaign ID'] = ''
    data['Note - Missing Placement ID'] = ''
    data['Note - Incorrect Start Date'] = ''
    data['Note - Missing Tag Type'] = ''
    data['Note - Zero IO Rate'] = ''
    
    try:
        data.loc[data['DCM Placement ID'].isna(), ['Note - Missing Placement ID']] = 'x'
        data.loc[data['DCM Placement ID'].isna(), ['DCM Placement ID']] = 'Missing Placement ID'
    except:
        print('Placment ID no problem!')

    try:
        data.loc[data['Campaign ID'].isna(), ['Note - Missing Campaign ID']] = 'x'
        data.loc[data['Campaign ID'].isna(), ['Campaign ID']] = 'Missing Campaign ID'
    except:
        print('Campaign ID no problem!')
        
    try:
        data.loc[data['Start Date'] == 'TBD', ['Note - Incorrect Start Date']] = 'x'
    except:
        print('No TBD found!')
    try:
        data.loc[data['Start Date'].isna(), ['Note - Incorrect Start Date']] = 'x'
    except:
        print('No missing value found!')
    try:
        data.loc[(data['Start Date'] > datetime.strptime('2020-06-29', '%Y-%m-%d')) &
                 (data['Brand'] != 'Amazon Cross Quarter'), ['Note - Incorrect Start Date']] = 'x'
        data.loc[(data['Start Date'] < datetime.strptime('2020-03-30', '%Y-%m-%d')) &
                 (data['Brand'] != 'Amazon Cross Quarter'), ['Note - Incorrect Start Date']] = 'x'
    except:
        print('start date all are proper!')
        
    data.loc[(data['Adserving Fees - Tag Type'].isna()) & (data['Width'] != '2'), ['Note - Missing Tag Type']] = 'x'
    data.loc[(data['Adserving Fees - Tag Type'].isna()) & (data['Width'] != '2'), ['Adserving Fees - Tag Type']] = 'Missing Tag Type'
    
    data.loc[(data['Net/Gross Rate'].isna()) & (data['Cost Structure'].str.lower() != 'vadd') & 
         (data['Cost Structure'].str.lower() != 'flat rate - impressions') & (data['Placement Type'] == 'Package'),['Note - Zero IO Rate']] = 'x'
    data.loc[(data['Net/Gross Rate'] == 0) & (data['Cost Structure'].str.lower() != 'vadd') & 
             (data['Cost Structure'].str.lower() != 'flat rate - impressions') & (data['Placement Type'] == 'Package'),['Note - Zero IO Rate']] = 'x'
    data.loc[(data['Net/Gross Rate'].isna()) & (data['Cost Structure'].str.lower() != 'vadd') & 
             (data['Cost Structure'].str.lower() != 'flat rate - impressions') & (data['Placement Type'] == 'Package'),['Net/Gross Rate']] = 'Zero IO Rate'
    data.loc[(data['Net/Gross Rate'] == 0) & (data['Cost Structure'].str.lower() != 'vadd') & 
             (data['Cost Structure'].str.lower() != 'flat rate - impressions') & (data['Placement Type'] == 'Package'),['Net/Gross Rate']] = 'Zero IO Rate'
    data.loc[data['Cost Structure'] == 'CPE', 'Cost Structure'] = 'CPA'
    
    problematic_rows = data.loc[(data['Note - Missing Campaign ID'] == 'x') | 
                            (data['Note - Missing Placement ID'] == 'x') |
                            (data['Note - Incorrect Start Date'] == 'x') |
                            (data['Note - Missing Tag Type'] == 'x') |
                            (data['Note - Zero IO Rate'] == 'x')]
    
    os.chdir(output_location_true)
    files = os.listdir()
    for f in files:
        os.remove(f)
    problematic_rows.to_csv('Clorox AMJ Problematic File.csv', index = False)
    data.to_csv('Clorox AMJ BD Master File.csv', index = False)
    clear_output(wait=True)
    print('Done!')
    return problematic_rows


# In[121]:


# Clorox FY21 JAS

def Clorox_JAS_BD_download(Clorox_BD_ID, Amazon_Cross_Quarter, file_save_location, output_location, drive):
    
    file_save_location_true = file_save_location.replace('[\]','[\\]')
    output_location_true = output_location.replace('[\]','[\\]')
    Clorox_BD_ID = Clorox_BD_ID
    Amazon_Cross_Quarter = Amazon_Cross_Quarter
    
    clorox_list = folder_retrieval(Clorox_BD_ID, drive)
    FY21_ID = clorox_list[clorox_list.name == 'FY21'].id.to_string(index = False).strip()
    FY21_list = folder_retrieval(FY21_ID, drive)
    JAS_ID = FY21_list[FY21_list.name == '1. JAS'].id.to_string(index = False).strip()
    JAS_list = folder_retrieval(JAS_ID, drive)

    Amazon_ID = clorox_list[clorox_list.name == 'Amazon'].id.to_string(index = False).strip()
    Amazon_list = folder_retrieval(Amazon_ID, drive)
    Amazon_JAS = Amazon_list[Amazon_list.name == 'FY21 JAS'].id.to_string(index = False).strip()
    Amazon_Cross_Quarter = Amazon_list[Amazon_list.name == 'Cross-Quarter'].id.to_string(index = False).strip()
    Amazon_JAS_list = folder_retrieval(Amazon_JAS, drive)
    Amazon_Cross_Quarter_list = folder_retrieval(Amazon_Cross_Quarter, drive)
    
    File_Name = []
    Brand = []
    for i in range(len(JAS_list)):
        brand_id = JAS_list.id.iloc[i]
        files = folder_retrieval(brand_id, drive)
        if len(files)>0:
            files = files[files.name != 'Archive']
            files = files[files.name != 'Archived']
            for j in range(len(files)):
                if files.name.iloc[j] != 'Untitled spreadsheet':
                    File_Name.append(files.name.iloc[j])
                    Brand.append(JAS_list.name.iloc[i])
                    file_id = files.id.iloc[j]
                    temp = drive.CreateFile({'id': file_id})
                    clear_output(wait = True)
                    print('Downloading file:  %s' % files.name.iloc[j]) 
                    temp.GetContentFile(files.name.iloc[j])

    for i in range(len(Amazon_JAS_list)):
        brand_id = Amazon_JAS_list.id.iloc[i]
        files = folder_retrieval(brand_id, drive)
        if len(files)>0:
            files = files[files.name != 'Archive']
            files = files[files.name != 'Archived']
            for j in range(len(files)):
                if files.name.iloc[j] != 'Untitled spreadsheet':
                    File_Name.append(files.name.iloc[j])
                    Brand.append('Amazon')
                    file_id = files.id.iloc[j]
                    temp = drive.CreateFile({'id': file_id})
                    clear_output(wait = True)
                    print('Downloading file:  %s' % files.name.iloc[j]) 
                    temp.GetContentFile(files.name.iloc[j])

    for i in range(len(Amazon_Cross_Quarter_list)):
        brand_id = Amazon_Cross_Quarter_list.id[i]
        files = folder_retrieval(brand_id, drive)
        if len(files)>0:
            files = files[files.name.str.lower() != 'archive']
            files = files[files.name.str.lower() != 'archived']
            for j in range(len(files)):
                if files.name.iloc[j] != 'Untitled spreadsheet':
                    if 'folder' in files.type.iloc[j]:
                        folder_id = files.id.iloc[j]
                        sub_files = folder_retrieval(folder_id, drive)
                        if len(sub_files)>0:
                            sub_files = sub_files[sub_files.name.str.lower() != 'archive']
                            sub_files = sub_files[sub_files.name.str.lower() != 'archived']
                            for k in range(len(sub_files)):
                                if sub_files.name.iloc[k] != 'Untitled spreadsheet':
                                    if 'folder' in sub_files.type.iloc[k]:
                                        folder_id_2 = sub_files.id.iloc[k]
                                        sub_files_2 = folder_retrieval(folder_id_2, drive)
                                        if len(sub_files)>0:
                                            sub_files_2 = sub_files_2[sub_files_2.name.str.lower() != 'archive']
                                            sub_files_2 = sub_files_2[sub_files_2.name.str.lower() != 'archived']
                                            for l in range(len(sub_files_2)):
                                                File_Name.append(sub_files_2.name.iloc[l])
                                                Brand.append('Amazon Cross Quarter')
                                                sub_file_2_id = sub_files_2.id.iloc[l]
                                                temp = drive.CreateFile({'id': sub_file_2_id})
                                                clear_output(wait=True)
                                                print('Downloading file:  %s' % sub_files_2.name.iloc[l]) 
                                                temp.GetContentFile(sub_files_2.name.iloc[l])
                                    else:
                                        File_Name.append(sub_files.name.iloc[k])
                                        Brand.append('Amazon Cross Quarter')
                                        sub_file_id = sub_files.id.iloc[k]
                                        temp = drive.CreateFile({'id': sub_file_id})
                                        clear_output(wait=True)
                                        print('Downloading file:  %s' % sub_files.name.iloc[k]) 
                                        temp.GetContentFile(sub_files.name.iloc[k])
                    else:
                        File_Name.append(files.name.iloc[j])
                        Brand.append('Amazon Cross Quarter')
                        file_id = files.id.iloc[j]
                        temp = drive.CreateFile({'id': file_id})
                        clear_output(wait=True)
                        print('Downloading file:  %s' % files.name.iloc[j]) 
                        temp.GetContentFile(files.name.iloc[j])

    Reference = pd.DataFrame({'file name': File_Name, 'brand': Brand})
    Reference.to_csv('Reference.csv', index = False)
    
    file_list = glob.glob('*.xlsm')
    master = file_combine(file_list)
    
    reference = pd.read_csv('Reference.csv')
    reference.rename(columns={'file name':'Buy Details', 'brand':'Brand'}, inplace=True)
    
    clear_output(wait=True)
    print('Cleaning up data......')
    master_copy = master
    master = master[master['Geo'] != '']
    master = master.merge(reference)
    
    data = master
    Size = []
    for i in range(len(data)):
        if data['Placement Type'][i] == 'Package':
            Size.append('PKG')
        elif data['Placement Type'][i] == 'PKG':
            Size.append('PKG')
        elif ((data['Width'][i] == 'Vast') | (data['Width'][i] == 'VAST')):
            Size.append('0 x 0') 
        else:
            Size.append((str(data['Width'][i]) + ' x ' + str(data['Height'][i])).split('.')[0])
    data['Size'] = Size
    
    columns_needed = data.loc[:,['Line Item', 'Geo','Site Name','audience + placement name','Vehicle','Cost Structure',
                            'Campaign ID','Inventory Source','Targetin WHO','Size','Site Served or Dart']]
    data['DCM Placement Name'] = columns_needed.apply(lambda x: '|'.join(x.astype(str).values), axis=1)
    data['DCM Placement Name'].replace({'\.0': ''}, inplace=True, regex=True)
    
    data['Note - Missing Campaign ID'] = ''
    data['Note - Missing Placement ID'] = ''
    data['Note - Incorrect Start Date'] = ''
    data['Note - Missing Tag Type'] = ''
    data['Note - Zero IO Rate'] = ''
    
    try:
        data.loc[data['DCM Placement ID'].isna(), ['Note - Missing Placement ID']] = 'x'
        data.loc[data['DCM Placement ID'].isna(), ['DCM Placement ID']] = 'Missing Placement ID'
    except:
        print('Placment ID no problem!')

    try:
        data.loc[data['Campaign ID'].isna(), ['Note - Missing Campaign ID']] = 'x'
        data.loc[data['Campaign ID'].isna(), ['Campaign ID']] = 'Missing Campaign ID'
    except:
        print('Campaign ID no problem!')
        
    try:
        data.loc[data['Start Date'] == 'TBD', ['Note - Incorrect Start Date']] = 'x'
    except:
        print('No TBD found!')
    try:
        data.loc[data['Start Date'].isna(), ['Note - Incorrect Start Date']] = 'x'
    except:
        print('No missing value found!')
    try:
        data.loc[(data['Start Date'] > datetime.strptime('2020-09-27', '%Y-%m-%d')) &
                 (data['Brand'] != 'Amazon Cross Quarter'), ['Note - Incorrect Start Date']] = 'x'
        data.loc[(data['Start Date'] < datetime.strptime('2020-06-29', '%Y-%m-%d')) &
                 (data['Brand'] != 'Amazon Cross Quarter'), ['Note - Incorrect Start Date']] = 'x'
    except:
        print('start date all are proper!')
        
    data.loc[(data['Adserving Fees - Tag Type'].isna()) & (data['Width'] != '2'), ['Note - Missing Tag Type']] = 'x'
    data.loc[(data['Adserving Fees - Tag Type'].isna()) & (data['Width'] != '2'), ['Adserving Fees - Tag Type']] = 'Missing Tag Type'
    
    data.loc[(data['Net/Gross Rate'].isna()) & (data['Cost Structure'].str.lower() != 'vadd') & 
         (data['Cost Structure'].str.lower() != 'flat rate - impressions') & (data['Placement Type'] == 'Package'),['Note - Zero IO Rate']] = 'x'
    data.loc[(data['Net/Gross Rate'] == 0) & (data['Cost Structure'].str.lower() != 'vadd') & 
             (data['Cost Structure'].str.lower() != 'flat rate - impressions') & (data['Placement Type'] == 'Package'),['Note - Zero IO Rate']] = 'x'
    data.loc[(data['Net/Gross Rate'].isna()) & (data['Cost Structure'].str.lower() != 'vadd') & 
             (data['Cost Structure'].str.lower() != 'flat rate - impressions') & (data['Placement Type'] == 'Package'),['Net/Gross Rate']] = 'Zero IO Rate'
    data.loc[(data['Net/Gross Rate'] == 0) & (data['Cost Structure'].str.lower() != 'vadd') & 
             (data['Cost Structure'].str.lower() != 'flat rate - impressions') & (data['Placement Type'] == 'Package'),['Net/Gross Rate']] = 'Zero IO Rate'
    data.loc[data['Cost Structure'] == 'CPE', 'Cost Structure'] = 'CPA'
    
    problematic_rows = data.loc[(data['Note - Missing Campaign ID'] == 'x') | 
                            (data['Note - Missing Placement ID'] == 'x') |
                            (data['Note - Incorrect Start Date'] == 'x') |
                            (data['Note - Missing Tag Type'] == 'x') |
                            (data['Note - Zero IO Rate'] == 'x')]
    
    os.chdir(output_location_true)
    files = os.listdir()
    for f in files:
        os.remove(f)
    problematic_rows.to_csv('Clorox JAS Problematic File.csv', index = False)
    data.to_csv('Clorox JAS BD Master File.csv', index = False)
    clear_output(wait=True)
    print('Done!')
    return problematic_rows


# In[ ]:


# Clif

def Clif_BD_download(Clif_BD_ID, file_save_location, output_location, drive):
    Clif_list = folder_retrieval(Clif_BD_ID, drive)
    Clif_list = Clif_list[Clif_list.name != 'Master Template']

    os.chdir(file_save_location.replace('[\]','[\\]'))
    File_Name = []
    Brand = []

    for i in range(len(Clif_list)):
        brand_id = Clif_list.id.iloc[i]
        files = folder_retrieval(brand_id, drive)
        if len(files)>0:
            files = files[files.name != 'Archive']
            files = files[files.name != 'Archived']
            for j in range(len(files)):
                File_Name.append(files.name.iloc[j])
                Brand.append(Clif_list.name.iloc[i])
                file_id = files.id.iloc[j]
                temp = drive.CreateFile({'id': file_id})
                clear_output(wait=True)
                print('Downloading file:  %s' % files.name.iloc[j]) 
                temp.GetContentFile(files.name.iloc[j])
    Reference = pd.DataFrame({'file name': File_Name, 'brand': Brand})
    Reference.to_csv('Reference.csv', index = False)
    
    file_list = glob.glob('*.xlsm')
    master = file_combine(file_list)
    
    reference = pd.read_csv('Reference.csv')
    reference.rename(columns={'file name':'Buy Details', 'brand':'Brand'}, inplace=True)
    
    master_copy = master
    master = master[master['Geo'] != '']
    master = master.merge(reference)
    data = master

    Size = []
    for i in range(len(data)):
        if data['Placement Type'][i] == 'Package':
            Size.append('PKG')
        elif data['Placement Type'][i] == 'PKG':
            Size.append('PKG')
        elif ((data['Width'][i] == 'Vast') | (data['Width'][i] == 'VAST')):
            Size.append('0 x 0') 
        else:
            Size.append((str(data['Width'][i]) + ' x ' + str(data['Height'][i])).split('.')[0])
    data['Size'] = Size
    
    columns_needed = data.loc[:,['Line Item', 'Geo','Site Name','audience + placement name','Vehicle','Cost Structure',
                            'Campaign ID','Inventory Source','Targetin WHO','Size','Site Served or Dart']]
    data['DCM Placement Name'] = columns_needed.apply(lambda x: '|'.join(x.astype(str).values), axis=1)
    data['DCM Placement Name'].replace({'\.0': ''}, inplace=True, regex=True)
    
    data['Note - Missing Campaign ID'] = ''
    data['Note - Missing Placement ID'] = ''
    data['Note - Incorrect Start Date'] = ''
    
    try:
        data.loc[data['DCM Placement ID'].isna(), ['Note - Missing Placement ID']] = 'x'
        data.loc[data['DCM Placement ID'].isna(), ['DCM Placement ID']] = 'Missing Placement ID'
    except:
        print('Placment ID no problem!')

    try:
        data.loc[data['Campaign ID'].isna(), ['Note - Missing Campaign ID']] = 'x'
        data.loc[data['Campaign ID'].isna(), ['Campaign ID']] = 'Missing Campaign ID'
    except:
        print('Campaign ID no problem!')
        
    try:
        data.loc[data['Start Date'] == 'TBD', ['Note - Incorrect Start Date']] = 'x'
    except:
        print('No TBD found!')
    try:
        data.loc[data['Start Date'].isna(), ['Note - Incorrect Start Date']] = 'x'
    except:
        print('No missing value found!')
        
    problematic_rows = data.loc[(data['Note - Missing Campaign ID'] == 'x') | 
                            (data['Note - Missing Placement ID'] == 'x') |
                            (data['Note - Incorrect Start Date'] == 'x')]
    
    os.chdir(output_location.replace('[\]','[\\]'))
    problematic_rows.to_csv('Clif Problematic File.csv', index = False)
    data.to_csv('Clif BD Master File.csv', index = False)
    
    clear_output(wait=True)
    print('Done!')
    return problematic_rows


# In[77]:


# Sending emails

def send_email(username, password, email_title, recipient, contents):
    yag = yagmail.SMTP(username, password)
    send_to = recipient
    contents = contents
    yag.send(send_to,email_title, contents)
    print('Email Sent!')

