##############################################################################
# Johnathan Clementi
# Advanced Python Programming for GIS - PSU GEOG 489
# Prof. James O’Brien, Grading Assistant Rossana Grzinic
# Final Project Deliverables
# Purpose: NJ Highlands Region annual preserved lands breakdown
###############################################################################

import os, sys
import arcpy
arcpy.env.overwriteOutput = True
arcpy.env.workspace = r'in_memory'
 
def worker(clipper, tobeclippeditem, field, oid, outFolder, name): 
    """  
       This is the function that does the work of clipping the input feature class to one of the polygons from the clipper feature class.  
       If the clip succeeds then it returns TRUE else FALSE.  
    """

    ''' Prep '''

    # Retrieve file location of current Feature Class - this is used to differentiate between Preserved Lands features and Planning/Preservation Region features
    tbcBseName = os.path.basename(tobeclippeditem)
  
    # Initialize a query string that will select the current municipality by OID 
    query = '"' + field +'" = ' + str(oid)

    # Initialize clipping municipality feature - use OID as a differentiating attribute (so workers dont use the same clippingMuni)
    clippingMuni = name + "clipper_" + str(oid)


    ''' 
        Create feature layer for a single municipality - this is used as the Clip feature in the clip operation 
        First parameter = (INPUT) feature class that holds clip features
        Second parameter = (OUTPUT) feature layer that is being created to hold single clip feature (in this case, one municipality)
        Third parameter = (INPUT) the query string created above - used to select municipality by its OID 
    '''
    arcpy.MakeFeatureLayer_management(clipper, clippingMuni, query) 
    
    # Initialize output feature layer name
    outFL = name + "_" + tbcBseName

    '''
        Conduct the clip
        First parameter = (INPUT) the current input layer (in this case either Preserved Lands or Planning / Preservation Regions)
        Second parameter = (INPUT) the current clip feature (refered to by the clipping municipality string created above)
        Third parameter = (OUTPUT) the output file being created 
    ''' 
    arcpy.Clip_analysis(tobeclippeditem, clippingMuni, outFL)


    # Create new fields for Municipality name and the area in acres
    arcpy.management.AddFields(outFL, [['MunName', 'TEXT', 200], ["Area", "FLOAT"]])

    # This is how to individually create fields
    #arcpy.AddField_management(outFL, "MunName", "TEXT")
    #arcpy.AddField_management(outFL, "Area", "FLOAT")

    # Add name of municipality to table
    arcpy.CalculateField_management(outFL, "MunName", '"' + name + '"')

    # Calculate the Acreage of the clip input polygons (preserved lands or ppRegions) in the new shapefile
    arcpy.CalculateField_management(outFL, "Area", "!shape.area@acres!")


    # Initialize name of output shapefile to go onto the disk
    outFC = os.path.join(outFolder, name + "_" + tbcBseName + ".shp")

    # Replacement for arcpy.env.overwriteOutput - check if output file exists, if it does, delete it
    if arcpy.Exists(outFC):
        arcpy.Delete_management(outFC)

    # Copy features from in memory to disk
    arcpy.CopyFeatures_management(outFL, outFC)
    
    # Initialize name for output csv file
    outCSV = name + "_" + tbcBseName + ".csv"

    # Check if csv exists, if it does, delete it, then recreate it
    if os.path.exists(outCSV):
        os.remove(outCSV)

    '''
        Write csv from attribute table for use in pandas
        First parameter = (INPUT) input table to be exported
        Second parameter = (OUTPUT) output path
        Third parameter = (OUTPUT) output file name
    '''
    arcpy.TableToTable_conversion(outFC, outFolder, outCSV)

    # Clean up in_memory
    arcpy.Delete_management(outFL) 
     
    arcpy.AddMessage("Finished clipping:" + str(oid)) 
    return True # everything went well so we return True
