# This script parses all DBT project folders found in root directory (see -r parameter) and generates report on materialization type used by each sql file. 
# Reprot is palced in two output files of same content. One in xlsx format another in pkl under same filename (see -o parameter) Both parameters are optional, 
# if not specified, default values are used.
#
# Use -h or --help option for help.
# 
# Composed by Leonid Tepliakov, leotepl@gmail.com


import os
import yaml
import glob
import re
import pandas as pd
import argparse

sep=os.sep #set path separator for current environment e.g ""/" for linux, "\"" for windows


# Parse input arguments

parser = argparse.ArgumentParser(
#                    prog='ProgramName',
                    description='Parses DBT projects in root folder and makes report on kind of materialization for each sql file. Report is placed in two files of same content in pkl and xlsx format.')
#                    epilog='Text at the bottom of help')
parser.add_argument('-r','--root', help="root directory where all project folders reside. Default current work directory. ")
parser.add_argument('-o','--out', help='Output file name. Default "DBT_Materializations"')
args=parser.parse_args()

if args.out:
    outputFileName=args.out
else:
    outputFileName="DBT_Materializations"

start_dir=os.getcwd()

if args.root:
    root=os.path.normpath(start_dir+sep+args.root)
else:
    root=os.getcwd()

# parse dbt_project.yml files in all projects folders found in root directory and build two dictionaries:
#   modelPathDefaultMaterialisations   for default materializations on project path level for each project as defined in model parameter of dbt_project.yml
#   projectPaths all model paths defined in model-paths parameter of dbt_project.yml

os.chdir(root)

projectDefaultMaterialisations={}
projectPaths={}

dirFiles=os.listdir(".")

#print (type(modelPathDefaultMaterialisations))
for name in dirFiles:         # loop by files in root directory
#    print(name,os.path.isdir(name))
    if os.path.isdir(name) and os. path. exists(name+sep+"dbt_project.yml"): # if file is a folder and contains dbt_project.yml - it is a project folder
 #       print (name+' is project')
        with open(name+sep+'dbt_project.yml', 'r') as file:
            projectParameters=yaml.safe_load(file)


        if 'models' in projectParameters: # if dbt_project.yml contains "models" parameter, parse it and append results to dictionary
            if projectParameters['models']:
                modelPathDefaultMaterialisations={}
                if name in projectParameters['models']:
#                    print (projectParameters['models'][name])
                    if projectParameters['models'][name]:
                        for k in projectParameters['models'][name].keys():
#                            print (k,'-',projectParameters['models'][name][k]['+materialized'])
                            modelPathDefaultMaterialisations[k]=projectParameters['models'][name][k]['+materialized']

        projectDefaultMaterialisations[name]=modelPathDefaultMaterialisations

        if 'model-paths' in projectParameters:  # if dbt_project.yml contains "model-paths" parameter, parse it and append results to dictionary
            if projectParameters['model-paths']:
                projectPaths[name]=projectParameters['model-paths']
            else:
                projectPaths[name]='models'
        else:
            projectPaths[name]='models'


regMaterialzation=re.compile(r"materialized='([a-z]+)'")  # compile regex to extract materialization from sql source

df = pd.DataFrame(columns=['Project Name', 'Object Type', 'File Path', 'File Name', 'File Extension','Materialized As']) #create empty dataframe

os.chdir(root)
globalMtr='veiw'
for project in projectPaths.keys():     # loop by projects
    for pth in projectPaths[project]:   # loop by model paths inside the project
 #       print(project+sep+pth)
        
        if os.path.isdir(project+sep+pth):  # if path defined in yml file does not really exist - ignore it
            projectPathMtr=globalMtr 
            projectPathOrig='global default'

                # if default materializatoin is set in yml model use this value as default for this path
            if project in projectDefaultMaterialisations.keys() \
            and pth in projectDefaultMaterialisations[project].keys() \
            and projectDefaultMaterialisations[project][pth]:
                projectPathMtr=projectDefaultMaterialisations[project][pth]
                projectPathOrig='project default'

            os.chdir(project+sep+pth)  # dive into model path folder and get the list of *.sql files
            l=glob.glob('**'+sep+'*.sql', recursive=True)
 
            for fl in l:                # loop by files in model path folder
                mtr=projectPathMtr
                orig=projectPathOrig
                with open(fl, 'r') as sqlFile:
                    sqlText=sqlFile.read()

                materialization=regMaterialzation.findall(sqlText)  # look if materialization is defined in the sql source
                if len(materialization)>0:
                    mtr=materialization[0]
                    orig='SQL source'
                                                # compose new row for result dataframe
                new_row={'Project Name':project,\
                         'Object Type':pth,\
                         'File Path':os.path.split(fl)[0],\
                         'File Name':os.path.splitext(os.path.split(fl)[1])[0],\
                         'File Extension':os.path.splitext(fl)[1].replace('.',''),\
                         'Materialized As':mtr,\
                         'Origin':orig,\
                        }
 #               print(project,' ',pth,' ',fl)
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            os.chdir(root)

os.chdir(start_dir)
df.to_pickle(outputFileName+".pkl") 
df.to_excel(outputFileName+".xlsx",index=False)
