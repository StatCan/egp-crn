# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Update_ExternalRef.py
# Created on: 2018-06-21
# Description: Updates the external reference file in NGDP1 for Ontario.
# Params: new FC, downloaded from LIO, old FC to be replaced
#
# brindusa.valachi@canada.ca
# ---------------------------------------------------------------------------

# Import arcpy module
import arcpy
import os, sys

def UpdateExternalRef(old, new):


    basePath = os.path.dirname(__file__)
    os.chdir(basePath)

    # original file from website in csv format
    oldFC = os.path.abspath(os.path.join(basePath, old))

    # transposed output
    newFC = os.path.abspath(os.path.join(basePath, new))


    """ get the correct path to the data  *.sde """

    arcpy.env.workspace = "C:\Users\bvalachi\Desktop\01_BVALACHI\NRN\FC.gdb"

    arcpy.env.overwriteOutput = True

    #check if file to be replaced exists

    # os.path.isfile

    if not arcpy.Exists(oldFC):
        print("{} does not exist. Failed to execute.".format(oldFC))
        #sys.exit(0)
        return


    # Local variables:
    ignore_options = "IGNORE_M;IGNORE_Z;IGNORE_POINTID;IGNORE_EXTENSION_PROPERTIES;IGNORE_SUBTYPES;IGNORE_RELATIONSHIPCLASSES;IGNORE_REPRESENTATIONCLASSES;IGNORE_FIELDALIAS"

    # output from Feature Compare tool.
    result_txt = os.path.abspath(os.path.join(basePath, "result.txt"))

    try:
        os.remove(result_txt)
    except OSError:
        pass

    try:
        Compare_Status = "true"

        # Process: Feature Compare. It checks for schema match

        arcpy.FeatureCompare_management(oldFC, newFC, "PR_UID", "SCHEMA_ONLY", ignore_options, \
            "0 DecimalDegrees", "2", "0", "", "", "NO_CONTINUE_COMPARE", \
            result_txt)
    except:
        print (arcpy.GetMessages())

    #check for schema match between old and new file: search for any TRUE values in Has_Errors field

    errors = 0
    msg = ""
    with open(result_txt) as f:
        for line in f:
            fields = line.split(",")
            if fields[0] == '"true"':   # Has_Errors field in the output
                errors = errors + 1
                msg = fields[2]         # Message field in the output

    if errors > 0:
        print("Error: {}. Failed to execute.".format(msg))
        return


    #check for schema lock before running Copy Features.

    if arcpy.TestSchemaLock(oldFC):
        arcpy.CopyFeatures_management(newFC, oldFC, "", "0", "0", "0")
    else:
        print("Error: Cannot get exclusive schema lock on {}.  Either being edited or in use by another application.".format(oldFC))
        return

    print ("Done!")



def main():

    if len(sys.argv) != 3:
        print ("Usage:  ext_ref.py <oldFC> <newFC>")

    else:
        UpdateExternalRef(sys.argv[1], sys.argv[2])



if __name__ == '__main__':
    main()
