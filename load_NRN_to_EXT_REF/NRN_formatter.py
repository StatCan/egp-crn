from __future__ import print_function, absolute_import
from __future__ import unicode_literals
import glob
import os
import re
import subprocess
import sys
import time

import arcpy
import yaml

def load_config(path):
    """Load configuration settings from file."""
    sys.stdout.write("Loading configuration from {}\n".format(path))
    with open(path) as yaml_file:
        try:
            cfg = yaml.load(yaml_file)
        except yaml.YAMLError as err:
            sys.stdout.write("Error loading configuration file\n")
            sys.exit(1)
        else:
            return cfg


def create_directory (path):
    """If work directory does not exist, create it."""
    if not os.path.isdir(path):
        sys.stdout.write('Creating Directory\n')
        os.makedirs(path)
    else:
        sys.stdout.write(path + ' Directory exists\n')


def create_workspace (dir_path, db_nme):
    """If workspace does not exist, create it."""
    path= os.path.join(dir_path, db_nme)
    if os.path.exists(path)==False:
        sys.stdout.write('Creating workspace\n')
        # Execute CreateFileGDB
        arcpy.CreateFileGDB_management(dir_path, db_nme)
        #set options
        arcpy.env.workspace =(path)
        arcpy.env.overwriteOutput = True
        arcpy.env.outputMFlag = "Disabled"
        arcpy.env.qualifiedFieldNames = False
    else:
        sys.stdout.write(path + ' workspace exists, setting up parameters\n')
        arcpy.env.workspace =(path)
        arcpy.env.overwriteOutput = True
        arcpy.env.outputMFlag = "Disabled"
        arcpy.env.qualifiedFieldNames = False


def nrn_dbf_processing(pr_uid, src_path, output_name):
    """import, join and purge fields from NRN dbf"""
    # Make a list of all DBFs provided NRN.
    nrn_dbf_files = glob.glob(src_path)
    pattern = r".*PR({}).*(STRPLANAME|ADDRANGE|ROADSEG).*".format(pr_uid)
    matching_pattern = re.compile(pattern)

    filtered_nrn_files = sorted([filepath for filepath in nrn_dbf_files if matching_pattern.match(filepath)])

    # Monkey Filepaths so they are separated and are correctly formatted for Spatialite commands.
    addrange = "{}".format(filtered_nrn_files[0].replace("\\", "\\\\"))
    roadseg = "{}".format(filtered_nrn_files[1].replace("\\", "\\\\"))
    strplaname = "{}".format(filtered_nrn_files[2].replace("\\", "\\\\"))

    #keepfield - fields to keep in each table
    kfields_seg="NID, ROADSEGID, ADRANGENID, PROVIDER, \
             ROADCLASS, RTNUMBER1, RTNUMBER2, RTENAME1FR, RTENAME2FR, \
             RTENAME1EN, RTENAME2EN, PAVSTATUS, L_HNUMF, L_HNUML, \
             L_STNAME_C, L_PLACENAM, R_HNUMF, R_HNUML, R_STNAME_C, R_PLACENAM"
    kfields_add= "NID, L_HNUMSTR, L_OFFNANID, L_ALTNANID, R_HNUMSTR, R_OFFNANID, R_ALTNANID"
    kfields_str_L="NID, DIRPREFIX as DIRPREFIX_L, STRTYPRE as STRTYPRE_L, STARTICLE as STARTICLE_L, \
                   NAMEBODY as NAMEBODY_L, STRTYSUF as STRTYSUF_L, DIRSUFFIX as DIRSUFFIX_L, \
                   MUNIQUAD as MUNIQUAD_L"
    kfields_str_R ="NID, DIRPREFIX as DIRPREFIX_R, STRTYPRE as STRTYPRE_R, STARTICLE as STARTICLE_R, \
                   NAMEBODY as NAMEBODY_R, STRTYSUF as STRTYSUF_R, DIRSUFFIX as DIRSUFFIX_R, \
                   MUNIQUAD as MUNIQUAD_R, PROVINCE"
    kfields_fin="NID, ROADSEGID, ADRANGENID, PROVIDER, \
                 ROADCLASS, RTNUMBER1, RTNUMBER2, RTENAME1FR, RTENAME2FR, \
                 RTENAME1EN, RTENAME2EN, PAVSTATUS, L_HNUMF, L_HNUML, \
                 L_STNAME_C, L_PLACENAM, R_HNUMF, R_HNUML, R_STNAME_C, R_PLACENAM, \
                 L_HNUMSTR, L_OFFNANID, L_ALTNANID, R_HNUMSTR, R_OFFNANID, R_ALTNANID, \
                 DIRPREFIX_L, STRTYPRE_L, STARTICLE_L, NAMEBODY_L, STRTYSUF_L, DIRSUFFIX_L, \
                 MUNIQUAD_L, DIRPREFIX_R, STRTYPRE_R, STARTICLE_R, \
                 NAMEBODY_R, STRTYSUF_R, DIRSUFFIX_R, MUNIQUAD_R, PROVINCE"

    #Delete existing table in memory
    #FIX, deletion works 1 time out of 2....
    commands = "PRAGMA writable_schema = 1; \
                delete from sqlite_master where type in ('table', 'index', 'trigger'); \
                PRAGMA writable_schema = 0; \n"

    #load the nrn dbf
    commands += ".loaddbf {addrange} ADDRANGE CP1252 \n".format(addrange=addrange)
    commands += ".loaddbf {roadseg} ROADSEG CP1252 \n".format(roadseg=roadseg)
    commands += ".loaddbf {strplaname} STRPLANAME CP1252 \n".format(strplaname=strplaname)

    #Keep only useful fields, duplicate STRPLANAME for left and right join
    commands += "CREATE TABLE ROADSEG_s AS SELECT {} FROM ROADSEG; \n".format(kfields_seg)
    commands += "CREATE TABLE ADDRANGE_s AS SELECT {} FROM ADDRANGE; \n".format(kfields_add)
    commands += "CREATE TABLE STRPLANAME_L AS SELECT {} FROM STRPLANAME; \n".format(kfields_str_L)
    commands += "CREATE TABLE STRPLANAME_R AS SELECT {} FROM STRPLANAME; \n".format(kfields_str_R)

    #Join all tables
    commands +="CREATE TABLE NRN_attribute AS \
                SELECT {} FROM \
                    (SELECT * \
                    FROM ROADSEG_s as A, ADDRANGE_s as B, STRPLANAME_L as C, STRPLANAME_R as D \
                    WHERE A.ADRANGENID=B.NID \
                    AND B.L_OFFNANID=C.NID \
                    AND B.R_OFFNANID=D.NID); \n".format(kfields_fin)

    #dumb into new dbf
    commands += ".dumpdbf NRN_attribute {output} CP1252 \n".format(output=output_name)

    proc = subprocess.Popen('C:\sqlite\spatialite.exe --echo', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.stdin.write(commands)
    proc.stdin.close()

    while proc.returncode is None:
        proc.poll()

    stdout = proc.stdout.read()
    stderr = proc.stderr.read()

    print(stdout)
    print(stderr)


def import_shp_to_fgdb(pr_uid, src_path, wkspc_path, prj_loc, Output_nme):
    """Copy NRN Provincial file to workspace and reproject."""

    if not arcpy.Exists(Output_nme):
        #Rummage for shapefile with ROADSEG and chosen PR_UID in their name
        nrn_shp_files = glob.glob(src_path)
        pattern = r".*PR({}).*(ROADSEG).*".format(pr_uid)
        matching_pattern = re.compile(pattern)
        nrn_shp_path = sorted([filepath for filepath in nrn_shp_files if matching_pattern.match(filepath)])

        #Fiddle with the path if one and only one shape match criteria, else throw a tantrum
        if len(nrn_shp_path)==1:
            shp_path= "{}".format(nrn_shp_path[0].replace("\\", "\\\\"))
            print(shp_path)
        elif len(nrn_shp_path)==0:
            raise IOError("Shapefile not found\n")
        else:
            raise IOError("More than one Shapefile found\n")

        #apply devious way to only keep needed fields then copy shape to workspace
        sys.stdout.write("{:.<50}".format("Importing NRN data to workspace"))

        fieldInfo=""
        fieldlist = arcpy.ListFields(shp_path)

        for field in fieldlist:
            if field.name == "OBJECTID":
                fieldInfo = fieldInfo + field.name + " " + field.name + " VISIBLE;"
            if field.name == "Shape":
                fieldInfo = fieldInfo + field.name + " " + field.name + " VISIBLE;"
            if field.name == "NID":
                fieldInfo = fieldInfo + field.name + " " + field.name + " VISIBLE;"
            if field.name == "Shape_Length":
                fieldInfo = fieldInfo + field.name + " " + field.name + " VISIBLE;"
            else:
                fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"

        arcpy.MakeFeatureLayer_management(shp_path,'stripped_shp','','', fieldInfo[:-1]);
        arcpy.CopyFeatures_management('stripped_shp','NRN_shp_tmp')

        #Reproject to Lambert
        sr = arcpy.SpatialReference(prj_loc)
        sys.stdout.write("{:.<50}".format("Reprojecting"))
        arcpy.management.Project('NRN_shp_tmp', Output_nme, sr)
        arcpy.Delete_management('NRN_shp_tmp')
        sys.stdout.write("Done\n")
    else:
        sys.stdout.write("Shp data already exists in workspace\n")


def join_geo_to_table (dset1_geo, dset2_table, joinfld1, joinfld2, result_nme):
    """Join a dataset to a table"""
    if not arcpy.Exists (result_nme):
        sys.stdout.write ("Joining {} geography to {} attribute table\n".format(dset1_geo, dset2_table))
        dtemp1=dset1_geo+'_T'

        arcpy.MakeFeatureLayer_management(dset1_geo, dtemp1)
        arcpy.MakeTableView_management(dset2_table,'attribute')

        arcpy.AddJoin_management(dtemp1,joinfld1,'attribute',joinfld2,'KEEP_ALL')
        arcpy.CopyFeatures_management(dtemp1, result_nme)

################################
def main():
    #import config from YML
    config = load_config('NRNconfig.yml')
    PRID = '{}'.format(config['data']['PR'])
    prj_loc = config['data']['PRJ']
    wkdir= config['data']['work_directory']
    nrn_root = config['data']['prov_data_directory']
    #generate variables for functions
    nrn_dbf_loc = nrn_root+".dbf"
    nrn_shp_loc = nrn_root+".shp"
    wkspc="NRN_to_EXTREF_PR{}.gdb".format(PRID)
    wkpath = os.path.join (wkdir, wkspc)
    shp_name="NRN_shp_PR{}".format(PRID)
    dbf_name = "attribute_PR{}.dbf".format(PRID)
    final_name="NRN_PR{}".format(PRID)
    attribute_dbf = os.path.join(wkdir, dbf_name)
    #run functions
    create_directory(wkdir)
    create_workspace(wkdir, wkspc)
    nrn_dbf_processing(PRID, nrn_dbf_loc, attribute_dbf)
    import_shp_to_fgdb(PRID, nrn_shp_loc, wkpath, prj_loc, shp_name)
    join_geo_to_table(shp_name, attribute_dbf, 'NID', 'NID', final_name)
    #clean up
    arcpy.DeleteField_management(final_name, 'NID_1')
    arcpy.Delete_management(shp_name)


if __name__ == '__main__':

    start_time = time.time()
    main()
    sys.stdout.write("Time Elapsed: {} minutes".format((time.time() - start_time)/60))
    sys.exit()


















