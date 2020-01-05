##############################################################################
# Johnathan Clementi
# Advanced Python Programming for GIS - PSU GEOG 489
# Prof. James O’Brien, Grading Assistant Rossana Grzinic
# Final Project Deliverables
# Purpose: NJ Highlands Region annual preserved lands breakdown
##############################################################################

''' Import necessary libraries '''
import os, sys
import re
import arcpy
arcpy.env.overwriteOutput = True # For testing purposes, allows us to overwrite old outputs
import multiprocessing
from workers import worker
import time
startTime = time.time()

# Set workspace to in memory to increase efficiency
arcpy.env.workspace = r'in_memory'


''' Data Input/Output'''

# Municipalities of New Jersey:
# https://njogis-newjersey.opendata.arcgis.com/datasets/3d5d1db8a1b34b418c331f4ce1fd0fef_2
njMuni = r'C:\Users\Johnathan\Google Drive\Grad School\PSU_GIS_Cert\GEOG 489\FinalPrj\data\HighlandsProtectedLands.gdb\NJ_Municipalities'

# Highlands Region
# http://highlands-data-njhighlands.opendata.arcgis.com/datasets/highlands-boundary
highlandsBoundary = r'C:\Users\Johnathan\Google Drive\Grad School\PSU_GIS_Cert\GEOG 489\FinalPrj\data\HighlandsProtectedLands.gdb\Highlands_Boundary'

# Municipalities of the Highlands Region (NJ_Municipalities clipped to Highlands_Boundary)
# Note: There are two 'Washington Townships' within the Highlands Region
highlandsMuni = r'C:\Users\Johnathan\Google Drive\Grad School\PSU_GIS_Cert\GEOG 489\FinalPrj\data\HighlandsProtectedLands.gdb\highlandsMuni'

# Planning and Preservation Designations
# http://highlands-data-njhighlands.opendata.arcgis.com/datasets/preservation-and-planning-area
planPresPoly = r'C:\Users\Johnathan\Google Drive\Grad School\PSU_GIS_Cert\GEOG 489\FinalPrj\data\HighlandsProtectedLands.gdb\Preservation_and_Planning_Area'

# Preserved Lands within the Highlands Region
# http://highlands-data-njhighlands.opendata.arcgis.com/datasets/preserved-lands
presLands = r'C:\Users\Johnathan\Google Drive\Grad School\PSU_GIS_Cert\GEOG 489\FinalPrj\data\HighlandsProtectedLands.gdb\Preserved_Lands'


# Input feature classes - on disk
# clipper = highlandsMuni 
# tobeclipped = [presLands, planPresPoly]

# Output directory
outFolder = r'C:\Users\Johnathan\Google Drive\Grad School\PSU_GIS_Cert\GEOG 489\FinalPrj\data\output'

# Check if output directory exists. Create a directory if one does not exist
if os.path.exists(outFolder):
    if os.path.isdir(outFolder):
        print('The proper output folder exists, moving on')
    else:
        os.mkdir(outFolder)
        print('Created the output directory')
else: 
    os.mkdir(outFolder)
    print('Created the output directory')



''' In Memory Data '''

# Make an in_memory feature layer for clip feature which is the Highlands Municipalities
clipper = "in_memory" + "\\" + "highlandsMuni"
arcpy.MakeFeatureLayer_management(highlandsMuni, clipper)

# Make an in_memory feature layer for Preserved lands
inMemPresLands = "in_memory" + "\\" + "Preserved_Lands"
arcpy.MakeFeatureLayer_management(presLands, inMemPresLands)

# Make an in_memory feature layer for Planning/Preservation Regions
inMemPlanPresPoly = "in_memory" + "\\" + "Preservation_and_Planning_Area"
arcpy.MakeFeatureLayer_management(planPresPoly, inMemPlanPresPoly)

# Add in memory preserved lands and planning/preservation regions to tobeclipped list
tobeclipped = [inMemPresLands, inMemPlanPresPoly]


''' Check for and use 64 bit processing '''

def get_install_path():
    ''' Return 64bit python install path from registry (if installed and registered),
        otherwise fall back to current 32bit process install path.
    '''
    if sys.maxsize > 2**32: return sys.exec_prefix #We're running in a 64bit process
  
    #We're 32 bit so see if there's a 64bit install
    path = r'SOFTWARE\Python\PythonCore\2.7'
  
    from _winreg import OpenKey, QueryValue
    from _winreg import HKEY_LOCAL_MACHINE, KEY_READ, KEY_WOW64_64KEY
  
    try:
        with OpenKey(HKEY_LOCAL_MACHINE, path, 0, KEY_READ | KEY_WOW64_64KEY) as key:
            return QueryValue(key, "InstallPath").strip(os.sep) #We have a 64bit install, so return that.
    except: return sys.exec_prefix #No 64bit, so return 32bit path 


''' Multiprocessing Handler Function '''

def mp_handler():
     
    try:
        
        print("Creating Polygon OID list...") 
      
        # These are the fields we want to grab from the clip feature layer
        field = ['OID@', 'MUN_LABEL']
        
         # Create a list of object IDs for clipper polygons
        idList = []

        # Initialize list of municipality names (municipalities are used as clip features)
        clipperNameList = []

        # Iterate through the rows of the municipality feature layer (clipper) and return the OID and name field data
        with arcpy.da.SearchCursor(clipper, field) as cursor:
            for row in cursor:
                id = row[0] # Retrieve OID from first element in row 
                name = row[1] # Retrieve Municipality name from second element in row
                name = name.replace(" ", "_") # Replace illegal characters so we can use this field as the name of the output file later on
                name = name.replace("-", "_")
                idList.append(id)
                clipperNameList.append(name)
     
        print("There are " + str(len(idList)) + " object IDs (polygons) to process.") 


        # Reset field variable to just that of the OIDFieldName of the municipality feature layer
        clipperDescObj = arcpy.Describe(clipper) 
        field = clipperDescObj.OIDFieldName


        # Initialize tuples (not list because tuples are immutable) of tasks that will be sent to workers 
        jobs = []

        '''
            Nested loop creates job list for each input feature layer of clip (preserved lands and planning/preservation regions) and each feature of clip feature layer
            Use enumerate to get index of tobeclipped list then assign value at that index to a variable holding one element (instead of a list)
        '''
        for i, item in enumerate (tobeclipped):
            tobeclippeditem = tobeclipped[i] # Get just one clip input feature layer
            j = 0 # Initialize index used for retrieving municipality name 
            for id in idList:
                name = clipperNameList[j] # Get municipality name from current index
                j += 1 # Advance municipality name index
                jobs.append((clipper,tobeclippeditem,field,id,outFolder, name)) # Add tuples of the parameters that need to be given to the worker function to the jobs list

        print("Job list has " + str(len(jobs)) + " elements.") 


        ''' Multiprocessing Pool '''

        # Create and run multiprocessing pool.
        multiprocessing.set_executable(os.path.join(get_install_path(), 'pythonw.exe')) # make sure Python environment is used for running processes, even when this is run as a script tool

        print("Sending to pool") 

        cpuNum = multiprocessing.cpu_count()  # determine number of cores to use
        print("There are: " + str(cpuNum) + " cpu cores on this machine") 

        with multiprocessing.Pool(processes=cpuNum) as pool: # Create the pool object 
            res = pool.starmap(worker, jobs)  # run jobs in job list; res is a list with return values of the worker function


        ''' Error Reporting if successful try '''
 
        failed = res.count(False) # count how many times False appears in the list with the return values
        if failed > 0:
            arcpy.AddError("{} workers failed!".format(failed)) 
            print("{} workers failed!".format(failed)) 


        # If the process was completed, print a message 
        arcpy.AddMessage("Finished multiprocessing!") 
        print("Finished multiprocessing!")

        # Clean up in_memory
        arcpy.Delete_management("in_memory") 

        # Print processing time
        arcpy.AddMessage("Total time: %s seconds" % (time.time() - startTime))
    

    
    # Error Reporting if unsuccessful try            
    except arcpy.ExecuteError:
        # Geoprocessor threw an error 
        arcpy.AddError(arcpy.GetMessages(2)) 
        print("Execute Error:", arcpy.ExecuteError) 
    except Exception as e: 
        # Capture all other errors 
        arcpy.AddError(str(e)) 
        print("Exception:", e)


    # Clean up in_memory
    arcpy.Delete_management("in_memory") 

    # Print processing time
    arcpy.AddMessage("Total time: %s seconds" % (time.time() - startTime))



''' Call multiprocessing handler function ''' 
if __name__ == '__main__':   
    mp_handler() 
